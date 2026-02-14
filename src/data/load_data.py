import sqlite3
import time
from typing import Literal, List

import pandas as pd
from nba_api.stats.endpoints import (
    commonallplayers,
    leaguegamefinder,
    playbyplayv3,
    playergamelog,
    playercareerstats,
)
from tqdm.auto import tqdm

from config import DB_PATH, logger, SLEEP_TIME, MAX_ERROR_TIMES


class NBALoader:
    """NBA 数据下载与存储器。

    该类负责与 NBA API 交互，并将获取的球员、球队及比赛数据进行分类存储。
    支持自动添加分类前缀，并具备基础的防重复下载逻辑。
    """

    def __init__(self):
        """初始化 NBALoader, 设置数据库路径."""
        self.db_path = DB_PATH

        self.sleep_time = SLEEP_TIME

        # 预定义核心表的关键约束
        self.table_schemas = {
            "player_info": "PERSON_ID INTEGER PRIMARY KEY",
            "player_stats": "PLAYER_ID INTEGER, SEASON_ID TEXT, TEAM_ID INTEGER",
            "game_log": "GAME_ID TEXT PRIMARY KEY",
            "game_pbp": "GAME_ID TEXT, EVENTNUM INTEGER, PRIMARY KEY (GAME_ID, EVENTNUM)",
        }

        logger.info("--- NBALoader initialized successfully ---")
        logger.info(f"--- Target database: {self.db_path} ---")

    def _get_existing_ids(self, full_table_name: str, id_column: str) -> set:
        """从数据库中获取已存在的唯一 ID 集合。

        Args:
            full_table_name: 完整的表名（如 player_stats）。
            id_column: 表中代表球员 ID 的列名（通常是 PLAYER_ID）。

        Returns:
            包含所有已存在 ID 的集合（set）。
        """
        try:
            with self._get_connection() as conn:
                query = f"SELECT DISTINCT {id_column} FROM {full_table_name}"
                df = pd.read_sql(query, conn)
                return set(df[id_column].tolist())
        except sqlite3.Error as e:
            logger.error(f"Database error when querying table {full_table_name}: {e}")
            return set()

    def _pause(self):
        time.sleep(self.sleep_time)

    def _get_connection(self):
        """创建一个数据库连接对象。"""
        try:
            return sqlite3.connect(self.db_path)
        except sqlite3.Error as e:
            logger.error(f"Cannot connect to database {self.db_path}: {e}")
            raise

    def _init_table_if_not_exists(self, table_name: str, df: pd.DataFrame):
        """如果表不存在，则根据 DataFrame 的结构自动创建。

        Args:
            table_name: 完整的表名（如 player_stats）。
            df: 包含数据样本的 DataFrame。
        """
        with self._get_connection() as conn:
            cursor = conn.cursor()

            # 检查表是否已经在数据库中
            cursor.execute(
                f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table_name}'"
            )

            if not cursor.fetchone():
                # 如果表不存在, 利用 pandas 存入 0 行数据来快速建立表头
                df.head(0).to_sql(table_name, conn, index=False, if_exists="replace")
                logger.info(f"Table structure initialized: {table_name}")

    def fetch_player_career(self, player_ids: List[int]):
        """批量获取并存储球员的职业生涯统计数据。

        该方法会自动过滤掉数据库中已经存在的球员 ID，实现增量存储。

        Args:
            player_ids: 待获取数据的球员 ID 列表。
        """
        full_table_name = "player_stats"

        # 检查数据库中已有的 ID
        existing_ids = self._get_existing_ids(full_table_name, "PLAYER_ID")

        # 过滤掉已存在的 ID，只下载“新”球员
        new_ids = [pid for pid in player_ids if pid not in existing_ids]

        if not new_ids:
            logger.info("No new player data to download")
            return

        logger.info(
            f"--- [Incremental Download] Preparing to fetch data for {len(new_ids)} new players ---"
        )

        pbar = tqdm(new_ids, desc="下载球员数据", unit="人")
        error_count = 0  # 错误计数器
        success_count = 0

        for pid in pbar:
            try:
                raw_data = playercareerstats.PlayerCareerStats(player_id=pid)
                df = raw_data.get_data_frames()[0]

                # 3. 调用通用保存方法，category="player", table_name="stats"
                # 拼接后即为 "player_stats"
                self._save_to_sqlite(
                    df, category="player", table_name="stats", if_exists="append"
                )

                success_count += 1
                # 礼貌性延迟，防止请求过快
                self._pause()

            except KeyboardInterrupt:
                logger.info("User interrupted the download process")
                break
            except Exception as e:
                error_count += 1
                logger.warning(f"Error downloading data for player {pid}: {e}")
                pbar.set_postfix(errors=error_count)
                continue

        logger.info(
            f"Player data download completed - Success: {success_count}, Failed: {error_count}"
        )

    def fetch_games(self, seasons: List[str] = "2023-24"):
        """批量获取指定赛季的全联盟比赛记录并存入数据库。

        Args:
            seasons: 赛季字符串列表，格式如 '2023-24'。
        """
        full_table_name = "game_log"
        logger.info(
            f"--- [Starting Download] Fetching league game records for seasons {seasons} ---"
        )

        error_count = 0
        success_count = 0

        pbar = tqdm(seasons, desc="下载赛季数据", unit="赛季")
        for season in pbar:
            # 访问休眠，防止封禁
            self._pause()

            try:
                game_finder = leaguegamefinder.LeagueGameFinder(
                    season_nullable=season, league_id_nullable="00"  # 00 表示NBA
                )
                df_all = game_finder.get_data_frames()[0]

                if df_all.empty:
                    logger.warning(f"No game data found for season {season}")
                    continue

                # 检查数据库中存在的 GAME_ID
                existing_game_ids = self._get_existing_ids(full_table_name, "GAME_ID")

                # 过滤掉已存在的比赛
                df_new = df_all[~df_all["GAME_ID"].isin(existing_game_ids)]

                if df_new.empty:
                    logger.info(f"All game data for season {season} already exists")
                    continue

                # 保存数据
                self._save_to_sqlite(
                    df_new, category="game", table_name="log", if_exists="append"
                )

                success_count += 1
                new_games_count = len(df_new)
                logger.info(f"Season {season} added {new_games_count} new game records")

            except KeyboardInterrupt:
                logger.info("User interrupted the download process")
                break
            except Exception as e:
                error_count += 1
                logger.error(f"Error downloading data for season {season}: {e}")
                pbar.set_postfix(errors=error_count, refresh=False)

        logger.info(
            f"Season data download completed - Success: {success_count}, Failed: {error_count}"
        )

    def fetch_player_game_logs(self, player_ids: List[int], seasons: List[str]):
        """获取特定球员的个人比赛日志

        支持多球员多赛季批量处理

        Args:
            player_ids: 球员 ID 的列表
            seasons: 赛季列表

        Returns:
            全部比赛 ID
        """
        full_table_name = "player_game_log"

        logger.info(
            f"--- Fetching game schedules for {len(player_ids)} players across seasons {seasons} ---"
        )

        all_game_ids = []
        total_new_games = 0
        success_count = 0

        error_count = 0
        players_pbar = tqdm(player_ids, desc="下载球员的比赛记录", unit="人")

        for player_id in players_pbar:
            self._pause()
            player_game_ids = []  # 当前球员的比赛ID

            seasons_bar = tqdm(seasons, desc=f"{player_id}", unit="赛季", leave=False)

            for season in seasons_bar:
                self._pause()
                try:
                    player_game_log = playergamelog.PlayerGameLog(
                        player_id=player_id, season=season
                    )

                    df = player_game_log.get_data_frames()[0]

                    if df.empty:
                        logger.debug(
                            f"No game data for player {player_id} in season {season}"
                        )
                        continue

                    # 检查数据库中已存在的 Game_ID
                    existing_game_ids = self._get_existing_ids(
                        full_table_name, "Game_ID"
                    )

                    # 过滤掉已存在的比赛记录
                    df_new = df[~df["Game_ID"].isin(existing_game_ids)]

                    if df_new.empty:
                        logger.debug(
                            f"Game data for player {player_id} in season {season} already exists"
                        )
                        continue

                    new_games_count = len(df_new)

                    self._save_to_sqlite(
                        df_new,
                        category="player",
                        table_name="game_log",
                        if_exists="append",
                    )

                    # 收集当前球员的比赛ID
                    season_game_ids = df_new["Game_ID"].unique().tolist()
                    player_game_ids.extend(season_game_ids)
                    all_game_ids.extend(season_game_ids)
                    total_new_games += new_games_count

                    success_count += 1
                    seasons_bar.set_description(
                        f"Player {player_id} (+{new_games_count} games)"
                    )

                except KeyboardInterrupt:
                    logger.info("User interrupted the download process")
                    return list(set(all_game_ids))
                except Exception as e:
                    error_count += 1
                    logger.warning(
                        f"Error downloading data for player {player_id} season {season}: {e}"
                    )
                    players_pbar.set_postfix(errors=error_count, refresh=False)
                    continue

        return list(set(all_game_ids))

    def fetch_pbp_data(self, game_ids: List[str]):
        full_table_name = "game_pbp"

        if not game_ids:
            logger.info(
                "--- [Info] Empty game ID list provided, skipping PBP download ---"
            )
            return

        # 检查 game_pbp 表中是否 已经存在这些比赛
        existing_pbp_ids = self._get_existing_ids(full_table_name, "gameId")

        # 过滤掉库里已有的比赛 ID
        todo_game_ids = [gid for gid in game_ids if gid not in existing_pbp_ids]

        if not game_ids:
            logger.info(
                f"--- [Info] All {len(game_ids)} games' PBP data already exists in database ---"
            )
            return

        logger.info(
            f"--- [Starting Download] Preparing to fetch PBP detail data for {len(todo_game_ids)} games ---"
        )

        all_new_data = []
        pbar = tqdm(todo_game_ids, desc="下载每场详细数据", unit="场")
        error_count = 0

        for gid in pbar:
            if error_count >= MAX_ERROR_TIMES:
                logger.warning(f"Error hits {MAX_ERROR_TIMES}, stop downloading")
                break

            self._pause()

            try:
                p_p = playbyplayv3.PlayByPlayV3(game_id=gid)
                df = p_p.get_data_frames()[0]

                if not df.empty:
                    all_new_data.append(df)

            except KeyboardInterrupt:
                logger.info("User interrupted the download process")
                break
            except Exception as e:
                error_count += 1
                logger.error(f"Error downloading PBP data for game {gid}: {e}")
                pbar.set_postfix(errors=error_count, refresh=False)
                continue

        if all_new_data:
            final_df = pd.concat(all_new_data, ignore_index=True)

            self._save_to_sqlite(
                final_df,
                category="game",
                table_name="pbp",
                if_exists="append",
            )

            logger.info(
                f"PBP download completed - Added {len(final_df)} rows, Errors: {error_count}"
            )
        else:
            logger.info("No new PBP data downloaded.")

    def get_local_player_game_ids(self, player_id: int) -> List[str]:
        """直接从本地数据库读取该球员已有的比赛 ID, 无需联网."""
        full_table_name = "player_game_log"

        try:
            with self._get_connection() as conn:
                # Filter for specific player ID
                query = f"SELECT DISTINCT Game_ID FROM {full_table_name} WHERE player_id = {player_id}"
                df = pd.read_sql(query, conn)
                return df["Game_ID"].tolist()
        except sqlite3.Error as e:
            logger.error(
                f"Database error when querying game IDs for player {player_id}: {e}"
            )
            return []
        except Exception as e:
            logger.error(
                f"Unknown error when querying game IDs for player {player_id}: {e}"
            )
            return []

    def _save_to_sqlite(
        self,
        df: pd.DataFrame,
        category: str,
        table_name: str,
        if_exists: Literal["fail", "replace", "append", "delete_rows"] = "append",
    ):
        """通用的数据保存方法。

        Args:
            df: 要保存的 Pandas DataFrame。
            category: 数据分类（如 'player', 'team'）。
            table_name: 具体表格名（如 'info', 'stats'）。
            if_exists: 如果表存在怎么处理 ('append', 'replace')。
        """
        if df.empty:
            logger.warning("Empty DataFrame received, skipping save")
            return

        # 自动构建带前缀的完整表名
        full_table_name = f"{category}_{table_name}"

        # 初始化表格 (确保列对齐)
        self._init_table_if_not_exists(full_table_name, df)

        # 写入数据
        try:
            with self._get_connection() as conn:
                df.to_sql(full_table_name, conn, if_exists=if_exists, index=False)
                if if_exists == "append":
                    logger.debug(
                        f"Successfully appended {len(df)} records to table {full_table_name}"
                    )
                else:
                    logger.debug(
                        f"Successfully saved {len(df)} records to table {full_table_name}"
                    )
        except sqlite3.Error as e:
            logger.error(f"Database error when writing to table {full_table_name}: {e}")
            raise
        except Exception as e:
            logger.error(f"Unknown error when writing to table {full_table_name}: {e}")
            raise

    def fetch_all_players(self):
        """获取全联盟球员基础信息列表并存入 player_info 表"""
        logger.info("Fetching all league player list from NBA API...")

        try:
            # is_only_current_season=0 to get all historical players
            raw_data = commonallplayers.CommonAllPlayers(is_only_current_season=0)
            df = raw_data.get_data_frames()[0]

            self._save_to_sqlite(
                df, category="player", table_name="info", if_exists="replace"
            )

            logger.info(
                f"Successfully fetched and saved basic info for {len(df)} players"
            )

        except Exception as e:
            logger.error(f"Failed to fetch player list: {e}")
            raise
