import sqlite3

import pandas as pd


class DatabaseManager:
    """负责书库的底层链接与原始查询"""

    def __init__(self, db_path):
        self.db_path = db_path
        self.conn = None

    def connect(self):
        """连接到数据库"""
        if not self.conn:
            self.conn = sqlite3.connect(self.db_path)
            print(f"✅ 已连接数据库: {self.db_path}")

    def query(self, sql, params=None):
        """在数据库里查询数据"""
        self.connect()
        return pd.read_sql_query(sql, self.conn, params=params)

    def close(self):
        """关闭数据库"""
        if self.conn:
            self.conn.close()
            self.conn = None
            print("🔌 数据库连接已关闭")
