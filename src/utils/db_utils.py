import os
import sqlite3

import pandas as pd

import config


class NBAAnalyzer:
    def __init__(self, db_path):
        self.db_path = db_path
        self.conn = None

    def connect(self):
        self.conn = sqlite3.connect(self.db_path)

    def disconnect(self):
        if self.conn:
            self.conn.close()

    def analyze_duration(self) -> pd.DataFrame:
        """
        [子任务：时长分析] 提取数据并自动保存
        """
        query = """
                SELECT g.season_id, \
                       SUBSTR(g.season_id, 2)                                                   AS season_year, \
                       AVG(CAST(SUBSTR(gi.game_time, 1, INSTR(gi.game_time, ':') - 1) AS INTEGER) * 60 + \
                           CAST(SUBSTR(gi.game_time, INSTR(gi.game_time, ':') + 1) AS INTEGER)) AS avg_duration
                FROM game g
                         JOIN game_info gi ON g.game_id = gi.game_id
                WHERE g.season_id LIKE '2%' \
                  AND gi.game_time LIKE '%:%'
                GROUP BY g.season_id \
                ORDER BY g.season_id ASC; \
                """
        df = pd.read_sql_query(query, self.conn)

        # 内部自动保存到对应的子文件夹
        save_path = os.path.join(
            config.DATA_PROCESSED, "duration", "reg_season_duration.csv"
        )
        df.to_csv(save_path, index=False)
        print(f"📁 数据已自动保存至: {save_path}")

        return df
