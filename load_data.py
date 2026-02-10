"""NBA 数据加载模块。

该模块封装了 NBALoader 类，用于通过 nba_api 获取数据并存储至本地 SQLite 数据库。
遵循 Google Python 编程规范。
"""

import sqlite3
import time
from typing import Set, List

import pandas as pd
from nba_api.stats.endpoints import (
    playercareerstats,
    commonallplayers,
    leaguegamefinder,
)

from config import DB_PATH


class NBALoader:
    """NBA 数据下载与存储器。

    该类负责与 NBA API 交互，并将获取的球员、球队及比赛数据进行分类存储。
    支持自动添加分类前缀，并具备基础的防重复下载逻辑。

    Attributes:
        db_path: 本地 SQLite 数据库的绝对路径。
    """

    def __init__(self):
        """初始化数据库链接"""
        self.db_path = DB_PATH
        self.table_schemas = {
            "player_info": "PERSON_ID INTEGER PRIMARY KEY",
            "player_stats": "PLAYER_ID INTEGER, SEASON_ID TEXT, TEAM_ID INTEGER",
            "game_log": "GAME_ID TEXT PRIMARY KEY, GAME_DATE TEXT, MATCHUP TEXT",
        }
        print(f"成功连接至数据库：{self.db_path}")

    def _init_table(self, full_table_name: str):
        """如果表不存在，则根据预定义的 Schema 创建表。

        Args:
            full_table_name: 完整的表名（前缀 + 名称）。
        """
        # 尝试匹配预定义的 schema，如果没有匹配到，则创建不带约束的简单表
        schema = self.table_schemas.get(full_table_name, "ID TEXT")
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            # 使用 SQL 原生指令，如果表存在则什么都不做
            cursor.execute(f"CREATE TABLE IF NOT EXISTS {full_table_name} ({schema})")
            conn.commit()

    def _get_existing_ids(self, table_name: str, id_column: str) -> Set[int]:
        """获取指定表中已存在的 ID 集合

        Args:
            table_name: 完整的数据库表明
            id_column: 代表唯一标识的列名

        Returns:
            包含所有已存在 ID 的集合.如果表不存在则返回空集
        """
        self._init_table(table_name)

        try:
            with sqlite3.connect(self.db_path) as conn:
                query = f"SELECT DISTINCT {id_column} FROM {table_name}"
                existing_df = pd.read_sql_query(query, conn)
                return set(existing_df[id_column].tolist())
        except (sqlite3.OperationalError, pd.io.sql.DatabaseError):
            return set()

    def _save_to_sqlite(
            self, df: pd.DataFrame, category: str, table_name: str, if_exists="append"
    ):
        """将 DataFrame 存储至指定的分类表中。

        Args:
            df: 待存储的数据。
            category: 分类前缀（如 'player', 'team', 'game'）。
            table_name: 具体表名（如 'info', 'stats'）。
            if_exists: 如果表已存在如何操作 ('append', 'replace', 'fail')。
        """
        full_table_name = f"{category}_{table_name}"
        self._init_table(full_table_name)

        if df.empty:
            print("数据为空, 跳过存储")
            return

        try:
            with sqlite3.connect(self.db_path) as conn:
                df.to_sql(full_table_name, conn, if_exists=if_exists, index=False)
            print(f"--- 成功更新表 [{table_name}]，新增 {len(df)} 行 ---")
        except Exception as e:
            print(f"写入数据库失败 ({full_table_name})：{e}")

    def fetch_all_players(self):
        """获取并更新全联盟球员基础信息。

        该操作将使用覆盖模式（replace）以确保球员最新状态（如所属球队）被同步。
        """
        print("正在获取全联盟球员基础信息...")
        # is_only_current_season=0 表示获取历史上所有球员
        all_players = commonallplayers.CommonAllPlayers(is_only_current_season=0)
        df = all_players.get_data_frames()[0]
        self._save_to_sqlite(
            df, "player", "info", if_exists="replace"
        )  # 基础表建议覆盖更新

    def fetch_player_career(self, player_ids: List[int]):
        """批量获取指定球员的职业生涯统计数据。

        该方法会自动过滤数据库中已存在的球员 ID，避免重复请求。

        Args:
            player_ids: 包含球员 ID 的列表。
        """
        existing_ids = self._get_existing_ids("player_stats", "PLAYER_ID")
        new_ids = [
            player_id for player_id in player_ids if player_id not in existing_ids
        ]

        if not new_ids:
            print("所有球员数据已存在, 跳过下载.")

        for player_id in new_ids:
            try:
                print(f"正在获取球员 ID {player_id} 的职业生涯数据...")
                career = playercareerstats.PlayerCareerStats(player_id=player_id)
                df = career.get_data_frames()[0]
                self._save_to_sqlite(df, "player", "stats")
                # 遵循 NBA API 频率限制
                time.sleep(1.0)
            except Exception as e:
                print(f"获取球员 {player_id} 失败: {e}")

    def fetch_recent_games(self, season: str = "2023-24"):
        """获取特定赛季的所有比赛记录。

        Args:
            season: 赛季字符串，格式如 '2023-24'。
        """
        print(f"正在获取 {season} 赛季的比赛记录...")
        game_finder = leaguegamefinder.LeagueGameFinder(season_nullable=season)
        df = game_finder.get_data_frames()[0]
        # 统一存放在 game_log 表中，后续通过分析代码过滤赛季
        self._save_to_sqlite(df, "game", "log")


if __name__ == "__main__":
    loader = NBALoader()

    # 1. 更新球员名单
    # loader.fetch_all_players()

    # 示例 2: 获取勒布朗的生涯数据 (ID: 2544)
    loader.fetch_player_career(player_ids=[2544, 201939])

    # 示例 3: 更新本赛季比赛
    # loader.fetch_recent_games(season="2023-24")
