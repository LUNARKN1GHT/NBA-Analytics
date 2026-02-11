import sqlite3
import time
from typing import Literal, List

import pandas as pd
from nba_api.stats.endpoints import (
    commonallplayers,
    leaguegamefinder,
    playbyplayv3,
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

    def fetch_play_by_play(self, game_ids: List[str]):
        full_table_name = "game_pbp"

        # 检查数据库中已有的 GAME_ID，避免重复下载
        existing_game_ids = self._get_existing_ids(full_table_name, "GAME_ID")

        # 过滤已下载的比赛记录
        new_game_ids = [gid for gid in game_ids if gid not in existing_game_ids]

        if not new_game_ids:
            print("--- [提示] 所有请求的比赛 PBP 数据已存在 ---")
            return

        print(f"--- [开始下载] 准备获取 {len(new_game_ids)} 场比赛的 PBP 数据 ---")

        for gid in new_game_ids:
            try:
                print(f"正在获取比赛 PBP：{gid}...")

                # 获取数据
                pbp = playbyplayv3.PlayByPlayV3(game_id=gid)
                dfs = pbp.get_data_frames()

                if not dfs or len(dfs) == 0:
                    print(f"比赛 {gid} 返回了空数据")
                    continue

                # 检查数据是否有效
                df = dfs[0]
                if df.empty:
                    print(f"比赛 {gid} 的PBP数据为空")
                    continue

                self._save_to_sqlite(
                    df, category="game", table_name="pbp", if_exists="append"
                )

                time.sleep(1.2)

            except Exception as e:
                print(f"获取比赛 {gid} 的 PBP 失败：{str(e)}[:100]")
                return

    def fetch_player_game_logs(self, player_id: int, seasons: List[str]):
        """获取特定球员的个人比赛日志"""
        full_table_name = "player_game_log"

        print(f"--- 正在获取球员 {player_id} 的 {seasons} 赛季赛程 ---")

        all_game_ids = []

        for season in seasons:
            try:
                player_game_log = playergamelog.PlayerGameLog(
                    player_id=player_id, season=season
                )
                df = player_game_log.get_data_frames()[0]

                if df.empty:
                    print(f"未找到球员 {player_id} 在 {season} 赛季的比赛记录。")
                    continue

                # 检查数据库中已存在的 Game_ID
                existing_game_ids = self._get_existing_ids(full_table_name, "Game_ID")

                # 过滤掉已存在的比赛记录
                df_new = df[~df["Game_ID"].isin(existing_game_ids)]

                if df_new.empty:
                    print(
                        f"--- [提示] 球员 {player_id} 在 {season} 赛季的所有比赛记录已存在 ---"
                    )
                    continue

                print(f"检测到 {season} 赛季的 {len(df_new)} 条新比赛记录，正在写入...")

                self._save_to_sqlite(
                    df_new, category="player", table_name="game_log", if_exists="append"
                )

                # 收集 Game_ID 用于返回
                all_game_ids.extend(df_new["Game_ID"].unique().tolist())

                # 礼貌性延迟，防止请求过快
                time.sleep(0.8)

            except Exception as e:
                print(f"获取球员赛程失败：{e}")
                continue

        if all_game_ids:
            print(
                f"--- 成功获取球员 {player_id} 共 {len(all_game_ids)} 场新比赛的 Game_ID ---"
            )
        else:
            print(f"--- 球员 {player_id} 在指定赛季内没有新的比赛记录 ---")

        return list(set(all_game_ids))  # 去重后返回

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
