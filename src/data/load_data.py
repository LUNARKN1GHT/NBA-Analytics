import sqlite3

import pandas as pd

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
