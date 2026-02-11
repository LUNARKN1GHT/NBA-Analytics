import sqlite3
import time
from typing import Literal, List

import pandas as pd
from nba_api.stats.endpoints import (
    commonallplayers,
    leaguegamefinder,
    playbyplay,
    playergamelog,
)

from config import DB_PATH


class NBALoader:
    """NBA 数据下载与存储器。

    该类负责与 NBA API 交互，并将获取的球员、球队及比赛数据进行分类存储。
    支持自动添加分类前缀，并具备基础的防重复下载逻辑。
    """

    def __init__(self):
        """初始化 NBALoader, 设置数据库路径."""
        self.db_path = DB_PATH

        self.sleep_time = 1.5

        # 预定义核心表的关键约束
        self.table_schemas = {
            "player_info": "PERSON_ID INTEGER PRIMARY KEY",
            "player_stats": "PLAYER_ID INTEGER, SEASON_ID TEXT, TEAM_ID INTEGER",
            "game_log": "GAME_ID TEXT PRIMARY KEY",
        }

        print("--- NBALoader 初始化成功 ---")
        print(f"--- 目标数据库: {self.db_path} ---")

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
                # 使用 SQL 的 DISTINCT 提高查询效率
                query = f"SELECT DISTINCT {id_column} FROM {full_table_name}"
                df = pd.read_sql(query, conn)
                return set(df[id_column].tolist())
        except Exception:
            print(f"有球员尚未创建: {Exception}")
            # 如果表还没创建，查询会报错，此时返回空集即可
            return set()

    def _pause(self):
        time.sleep(self.sleep_time)

    def _get_connection(self):
        """创建一个数据库连接对象。"""
        return sqlite3.connect(self.db_path)

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
                print(f"已自动初始化表结构: {table_name}")

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
            print(
                f"--- [提示] 所选的 {len(player_ids)} 位球员数据均已存在，跳过下载 ---"
            )
            return

        print(f"--- [增量下载] 准备获取 {len(new_ids)} 位新球员的数据 ---")

        for pid in new_ids:
            try:
                from nba_api.stats.endpoints import playercareerstats

                print(f"正在获取球员 ID: {pid} 的数据...")

                raw_data = playercareerstats.PlayerCareerStats(player_id=pid)
                df = raw_data.get_data_frames()[0]

                # 3. 调用通用保存方法，category="player", table_name="stats"
                # 拼接后即为 "player_stats"
                self._save_to_sqlite(
                    df, category="player", table_name="stats", if_exists="append"
                )

                # 礼貌性延迟，防止请求过快
                time.sleep(0.8)

            except Exception as e:
                print(f"获取球员 {pid} 失败: {e}")
                continue

    def fetch_games(self, seasons: List[str] = "2023-24"):
        """批量获取指定赛季的全联盟比赛记录并存入数据库。

        Args:
            seasons: 赛季字符串列表，格式如 '2023-24'。
        """
        full_table_name = "game_log"
        print(f"--- [开始下载] 正在获取 {seasons} 赛季的全联盟比赛记录 ---")

        for season in seasons:
            # 访问休眠，防止封禁
            time.sleep(1.0)

            try:
                game_finder = leaguegamefinder.LeagueGameFinder(
                    season_nullable=season, league_id_nullable="00"  # 00 表示NBA
                )
                df_all = game_finder.get_data_frames()[0]

                if df_all.empty:
                    print(f"未找到 {season} 赛季的任何比赛")
                    continue

                # 检查数据库中存在的 GAME_ID
                existing_game_ids = self._get_existing_ids(full_table_name, "GAME_ID")

                # 过滤掉已存在的比赛
                df_new = df_all[~df_all["GAME_ID"].isin(existing_game_ids)]

                if df_new.empty:
                    print(
                        f"--- [提示] {season} 赛季的所有比赛记录已在数据库中，无需更新 ---"
                    )
                    continue

                print(f"检测到 {season} 赛季的 {len(df_new)} 条新比赛记录，正在写入...")

                # 保存数据
                self._save_to_sqlite(
                    df_new, category="game", table_name="log", if_exists="append"
                )

            except Exception as e:
                print(f"获取比赛记录失败：{e}")

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

        print(f"--- 正在获取 {len(player_ids)} 名球员在 {seasons} 赛季的赛程 ---")

        all_game_ids = []
        total_new_games = 0

        for player_id in player_ids:
            self._pause()
            player_game_ids = []  # 当前球员的比赛ID

            for season in seasons:
                self._pause()
                try:
                    print(f"正在获取球员 {player_id} 的 {season} 赛季数据...")

                    player_game_log = playergamelog.PlayerGameLog(
                        player_id=player_id, season=season
                    )
                    df = player_game_log.get_data_frames()[0]

                    if df.empty:
                        print(f"未找到球员 {player_id} 在 {season} 赛季的比赛记录。")
                        continue

                    # 检查数据库中已存在的 Game_ID
                    existing_game_ids = self._get_existing_ids(
                        full_table_name, "Game_ID"
                    )

                    # 过滤掉已存在的比赛记录
                    df_new = df[~df["Game_ID"].isin(existing_game_ids)]

                    if df_new.empty:
                        print(
                            f"--- [提示] 球员 {player_id} 在 {season} 赛季的所有比赛记录已存在 ---"
                        )
                        continue

                    new_games_count = len(df_new)
                    print(
                        f"检测到 {season} 赛季的 {new_games_count} 条新比赛记录，正在写入..."
                    )

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

                except Exception as e:
                    print(f"获取球员 {player_id} 在 {season} 赛季的赛程失败：{e}")
                    continue

            # 每个球员处理完后汇报
            if player_game_ids:
                print(
                    f"--- 球员 {player_id} 处理完成，新增 {len(set(player_game_ids))} 场比赛记录 ---"
                )
            else:
                print(f"--- 球员 {player_id} 在指定赛季内没有新的比赛记录 ---")

        # 所有球员处理完后汇报总体情况
        unique_game_ids = list(set(all_game_ids))
        if unique_game_ids:
            print(
                f"--- 批量处理完成：共处理 {len(player_ids)} 名球员，"
                f"新增 {len(unique_game_ids)} 场不重复比赛，总计 {total_new_games} 条记录 ---"
            )
        else:
            print(
                f"--- 批量处理完成：共处理 {len(player_ids)} 名球员，无新增比赛记录 ---"
            )

        return unique_game_ids

    def fetch_pbp_data(self, game_ids: List[str]):
        full_table_name = "game_pbp"

        if not game_ids:
            print("--- [提示] 传入的比赛 ID 列表为空, 跳过 PBP 下载 ---")
            return

        # 检查 game_pbp 表中是否 已经存在这些比赛
        existing_pbp_ids = self._get_existing_ids(full_table_name, "GAME_ID")

        # 过滤掉库里已有的比赛 ID
        todo_game_ids = [gid for gid in game_ids if gid not in existing_pbp_ids]

        if not todo_game_ids:
            print(
                f"--- [提示] 传入的 {len(game_ids)} 场比赛的 PBP 均已存在于数据库中 ---"
            )
            return

        print(f"--- [开始下载] 准备获取 {len(todo_game_ids)} 场比赛的 PBP 细节数据 ---")

        success_count = 0
        for index, gid in enumerate(todo_game_ids):
            self._pause()

            try:
                print(f"[{index+1}/{len(todo_game_ids)}] 正在下载比赛 PBP: {gid}...")

                p_p = playbyplay.PlayByPlay(game_id=gid)
                df = p_p.get_data_frames()[0]

                if df.empty:
                    print(f"警告: 比赛 {gid} 返回了空 PBP 数据.")
                    continue

                self._save_to_sqlite(
                    df, category="game", table_name="pbp", if_exists="append"
                )

                success_count += 1

            except Exception as e:
                print(f"获取比赛 {gid} 的 PBP 失败: {e}")
                time.sleep(5)
                continue

        print(f"--- [下载完成] 成功获取 {success_count} 场比赛的 PBP 数据 ---")

    def get_local_player_game_ids(self, player_id: int) -> List[str]:
        """直接从本地数据库读取该球员已有的比赛 ID, 无需联网."""
        full_table_name = "player_game_log"

        try:
            with self._get_connection() as conn:
                # 筛选特定球员的 ID
                query = f"SELECT DISTINCT game_id FROM {full_table_name} WHERE player_id = {player_id}"
                df = pd.read_sql(query, conn)
                return df["Game_ID"].tolist()
        except Exception:
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
            print("警告: 收到空数据帧, 跳过保存. ")
            return

        # 自动构建带前缀的完整表名
        full_table_name = f"{category}_{table_name}"

        # 初始化表格 (确保列对齐)
        self._init_table_if_not_exists(full_table_name, df)

        # 写入数据
        try:
            with self._get_connection() as conn:
                df.to_sql(full_table_name, conn, if_exists=if_exists, index=False)
            print(f"--- 成功更新表 [{full_table_name}]，影响行数：{len(df)} ---")
        except Exception as e:
            print(f"错误：写入表 {full_table_name} 失败。具体原因: {e}")

    def fetch_all_players(self):
        """获取全联盟球员基础信息列表并存入 player_info 表"""
        print("正在从 NBA API 获取全联盟球员名单...")

        try:
            # is_only_current_season=0 获取历史所有球员
            raw_data = commonallplayers.CommonAllPlayers(is_only_current_season=0)
            df = raw_data.get_data_frames()[0]

            self._save_to_sqlite(
                df, category="player", table_name="info", if_exists="replace"
            )

        except Exception as e:
            print(f"获取球员名单失败: {e}")
