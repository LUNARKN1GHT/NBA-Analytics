from .base_analyzer import BaseAnalyzer


class GameAnalyzer(BaseAnalyzer):
    """比赛分析"""

    def analyze_game_duration_trend(self):
        """分析 NBA 比赛时长的年度趋势"""
        query = """
                SELECT g.season_id,
                       SUBSTR(g.season_id, 2)                                                   AS season_year,
                       AVG(CAST(SUBSTR(gi.game_time, 1, INSTR(gi.game_time, ':') - 1) AS INTEGER) * 60 +
                           CAST(SUBSTR(gi.game_time, INSTR(gi.game_time, ':') + 1) AS INTEGER)) AS avg_duration
                FROM game g
                         JOIN game_info gi ON g.game_id = gi.game_id
                WHERE g.season_id LIKE '2%'
                  AND gi.game_time LIKE '%:%'
                GROUP BY g.season_id \
                """
        return self.db.query(query)
