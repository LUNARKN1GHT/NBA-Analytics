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

    def analyze_home_advantage(self) -> pd.DataFrame:
        """分析 NBA 主场优势的情况"""
        query = """
                SELECT substr(season_id, 2)                               AS season_year,
                       count(*)                                           AS total_games,
                       sum(CASE WHEN wl_home = 'W' THEN 1 ELSE 0 END)     AS home_wins,
                       avg(CASE WHEN wl_home = 'W' THEN 1.0 else 0.0 END) AS home_win_pct,
                       AVG(pts_home - pts_away)                           AS avg_point_differential
                FROM game
                WHERE season_id LIKE '2%'
                GROUP BY season_id
                ORDER BY season_id;
                """
        df = pd.read_sql_query(query, self.conn)

        # 保存到对应的子文件夹
        save_path = os.path.join(
            config.DATA_PROCESSED,
            "home_advantage",
            "reg_season_home_advantage.csv",
        )
        df["home_win_pct"] = df["home_win_pct"] - 0.5
        df.to_csv(save_path, index=False)
        print(f"📁 数据已自动保存至: {save_path}")

        return df

    def analyze_clutch_shooters(self, min_attempts=20) -> pd.DataFrame:
        query = """
                SELECT p.player1_name      AS player_name,
                       -- 1. 统计该球员在该时刻下的所有投篮记录（包含命中和投丢）
                       COUNT(*)            AS clutch_attempts,

                       -- 2. 统计命中的次数
                       SUM(
                               CASE
                                   WHEN
                                       p.eventmsgtype = 1 -- 代码 1 指投篮命中
                                       THEN
                                       1
                                   ELSE
                                       0
                                   END
                       )                   AS clutch_made,

                       -- 3. 计算命中率：显式使用浮点数，确保纵轴不再是平直线
                       CAST(SUM(CASE WHEN p.eventmsgtype = 1 THEN 1 ELSE 0 END) AS FLOAT) /
                       COUNT(*)            AS clutch_fg_pct,

                       -- 4. 计算硬解命中（无助攻且命中）
                       SUM(CASE
                               WHEN p.eventmsgtype = 1 AND (p.player2_id IS NULL OR p.player2_id = 0) THEN 1
                               ELSE 0 END) AS isolator_made
                FROM play_by_play p
                WHERE p.period >= 4 -- 必须是第四节
                  -- 时间过滤：最后5分钟
                  AND (
                          CAST(SUBSTR(p.pctimestring, 1, INSTR(p.pctimestring, ':') - 1) AS INTEGER) * 60 +
                          CAST(SUBSTR(p.pctimestring, INSTR(p.pctimestring, ':') + 1) AS INTEGER)
                          ) <= 300
                  -- 分差过滤：5分以内
                  AND ABS(CAST(CASE WHEN p.scoremargin = 'TIE' THEN 0 ELSE p.scoremargin END AS INTEGER)) <= 5
                GROUP BY p.player1_id, p.player1_name
                HAVING clutch_attempts >= ? -- 纳入统计的最低出手标准
                ORDER BY clutch_attempts DESC;
                """
        df = pd.read_sql_query(query, self.conn, params=(min_attempts,))

        # 计算硬解率：非受助攻命中 / 总命中 (处理分母为0的情况)
        df["unassisted_rate"] = df["isolator_made"] / df["clutch_made"].replace(0, 1)

        # 清洗空值，确保绘图正常
        df = df.dropna(subset=["clutch_fg_pct", "clutch_attempts"]).copy()

        save_path = os.path.join(
            config.DATA_PROCESSED, "clutch", "top_clutch_shooters.csv"
        )

        os.makedirs(os.path.dirname(save_path), exist_ok=True)
        df.to_csv(save_path, index=False)

        print(f"📁 数据已自动保存至: {save_path}")
        return df
