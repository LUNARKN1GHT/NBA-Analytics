import sqlite3
from typing import Literal

import pandas as pd
from nba_api.stats.endpoints import commonallplayers

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
