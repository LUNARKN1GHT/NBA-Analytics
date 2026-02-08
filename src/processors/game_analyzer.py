from .base_analyzer import BaseAnalyzer


class GameAnalyzer(BaseAnalyzer):
    """比赛分析"""

    def analyze_game_duration_trend(self):
        """分析 NBA 比赛时长的年度趋势"""
        query = """
                SELECT
                    g.season_id,
                    SUBSTR(g.season_id, 2) AS season_year,
                    AVG(CAST(SUBSTR(gi.game_time, 1, INSTR(gi.game_time, ':') - 1) AS INTEGER) * 60 +
                        CAST(SUBSTR(gi.game_time, INSTR(gi.game_time, ':') + 1) AS INTEGER)) AS avg_duration
                FROM
                    game               g
                        JOIN game_info gi
                             ON g.game_id = gi.game_id
                WHERE
                    g.season_id LIKE '2%'
                    AND gi.game_time LIKE '%:%'
                GROUP BY
                    g.season_id
                """

        df = self.db.query(query)

        # 调整数据类型
        df["season_year"] = df["season_year"].astype(int)
        df["avg_duration"] = df["avg_duration"].astype(float)

        df["duration_ma3"] = df["avg_duration"].rolling(window=3, min_periods=3).mean()

        BaseAnalyzer._save_data(df, "game", "avg_duration")

        return df

    def home_advantage(self):
        """分析主场优势"""
        query = """
                WITH
                    home_stats AS (
                        SELECT
                            season_id,
                            team_abbreviation_home AS team,
                            COUNT(*) AS home_games,
                            SUM(CASE WHEN wl_home = 'W' THEN 1 ELSE 0 END) AS home_wins
                        FROM
                            game
                        WHERE
                            CAST(season_id AS INTEGER) / 10000 = 2
                        GROUP BY
                            season_id,
                            team_abbreviation_home
                                  ),
                    away_stats AS (
                        SELECT
                            season_id,
                            team_abbreviation_away AS team,
                            COUNT(*) AS away_games,
                            SUM(CASE WHEN wl_away = 'W' THEN 1 ELSE 0 END) AS away_wins
                        FROM
                            game
                        WHERE
                            CAST(season_id AS INTEGER) / 10000 = 2
                        GROUP BY
                            season_id,
                            team_abbreviation_away
                                  )
                SELECT
                    h.season_id,
                    CAST(h.season_id AS INTEGER) % 10000 AS season,
                    h.team,
                    h.home_games,
                    h.home_wins,
                    ROUND(h.home_wins * 100.0 / h.home_games, 2) AS home_win_rate,
                    a.away_games,
                    a.away_wins,
                    ROUND(a.away_wins * 100.0 / a.away_games, 2) AS away_win_rate,
                    -- 计算该赛季的主场优势差值
                    ROUND((h.home_wins * 100.0 / h.home_games) - (a.away_wins * 100.0 / a.away_games), 2) AS ha_diff
                FROM
                    home_stats          h
                        JOIN away_stats a
                             ON h.team = a.team AND h.season_id = a.season_id
                WHERE
                    h.home_games >= 10 -- 单个赛季主场至少10场才有意义
                ORDER BY
                    h.season_id DESC,
                    ha_diff DESC;
                """

        df = self.db.query(query)

        return df

    def three_attempt_trend(self):
        """分析历年三分出手比例"""
        query = """
                SELECT
                    -- 整理年份
                    CAST(season_id AS INTEGER) % 10000 AS season,

                    -- 统计出手数据
                    COALESCE(SUM(CAST(fgm_home AS INTEGER) + CAST(fgm_home AS INTEGER)), 0) AS fgm,
                    COALESCE(SUM(CAST(fga_home AS INTEGER) + CAST(fga_home AS INTEGER)), 0) AS fga,
                    COALESCE(SUM(CAST(fg3m_home AS INTEGER) + CAST(fg3m_home AS INTEGER)), 0) AS fg3m,
                    COALESCE(SUM(CAST(fg3a_home AS INTEGER) + CAST(fg3a_home AS INTEGER)), 0) AS fg3a,

                    -- 计算三分出手占比
                    CASE
                        WHEN COALESCE(SUM(CAST(fga_home AS INTEGER) + CAST(fga_away AS INTEGER)), 0) = 0
                            THEN 0
                        ELSE CAST(COALESCE(SUM(CAST(fg3a_home AS INTEGER) + CAST(fg3a_away AS INTEGER)), 0) AS FLOAT) /
                             CAST(COALESCE(SUM(CAST(fga_home AS INTEGER) + CAST(fga_away AS INTEGER)),
                                           0) AS FLOAT) END AS fg3a_pct,

                    CASE
                        WHEN COALESCE(SUM(CAST(fgm_home AS INTEGER) + CAST(fgm_away AS INTEGER)), 0) = 0
                            THEN 0
                        ELSE CAST(COALESCE(SUM(CAST(fg3m_home AS INTEGER) + CAST(fg3m_away AS INTEGER)), 0) AS FLOAT) /
                             CAST(COALESCE(SUM(CAST(fgm_home AS INTEGER) + CAST(fgm_away AS INTEGER)),
                                           0) AS FLOAT) END AS fg3m_pct
                FROM
                    game g
                GROUP BY
                    season"""

        df = self.db.query(query)

        return df
