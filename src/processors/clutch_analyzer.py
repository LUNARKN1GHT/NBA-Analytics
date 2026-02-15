"""关键时刻分析"""

import re
from typing import Dict

import pandas as pd

from src.processors.base_analyzer import BaseAnalyzer


def parse_v3_clock(clock_str):
    """将时间格式转换为秒数"""
    if not clock_str or not isinstance(clock_str, str):
        return 0
    m = re.search(r"PT(\d+)M", clock_str)
    s = re.search(r"M(\d+)", clock_str)
    minutes = int(m.group(1)) if m else 0
    seconds = float(s.group(1)) if s else 0.0
    return minutes * 60 + seconds


class ClutchAnalyzer(BaseAnalyzer):
    def analyze_player(
        self, player_id: int = None, player_name: str = None
    ) -> Dict | None:
        """核心出口：输入球员信息，直接返回该球员的关键时刻分析报告

        Args:
            player_id: 球员 ID
            player_name: 球员名称

        Returns:
            球员关键时刻数据报告
        """
        clutch_df = self.get_clutch_data(player_id=player_id, player_name=player_name)

        if clutch_df is None or clutch_df.empty:
            name = player_name if player_name else player_id
            print(f"--- [Warning] No clutch data found for {name} ---")
            return None

        metrics = self.calculate_metrics(clutch_df, player_id)

        metrics["Player"] = (
            player_name if player_name else clutch_df["playerName"].iloc[0]
        )
        metrics["Game_Count"] = clutch_df["gameId"].nunique()

        return metrics

    def calculate_metrics(self, clutch_df: pd.DataFrame) -> Dict | None:
        """计算关键时刻的指标

        Args:
            clutch_df: 关键时刻的表格

        Returns:
            关键时刻的相关指标
        """
        if clutch_df.empty:
            return {}

        # 投篮统计
        fga_df = clutch_df[clutch_df["isFieldGoal"] == 1]
        # 三分统计
        three_pa_df = fga_df[fga_df["shotValue"] == 3]
        # 罚球统计
        fta_df = clutch_df[
            clutch_df["actionType"].str.contains("Free Throw", case=False, na=False)
        ]

        # 计算命中数
        fgm = len(fga_df[fga_df["shotResult"].str.contains("Made")])
        three_pm = len(three_pa_df[three_pa_df["shotResult"].str.contains("Made")])
        ftm = len(
            fta_df[~fta_df["description"].str.contains("MISS", case=False, na=False)]
        )

        # 计算投篮数
        fga = len(fga_df)
        three_pa = len(three_pa_df)
        fta = len(fta_df)

        # 比例计算
        fg_pct = (fgm / fga) if fga > 0 else 0
        three_pct = (three_pm / three_pa) if three_pa > 0 else 0
        ft_pct = (ftm / fta) if fta > 0 else 0

        # 总得分统计
        total_pts = (fgm - three_pm) * 2 + three_pm * 3 + ftm

        # 真实命中率
        ts_pct = total_pts / (2 * (fga + 0.44 * fta)) if (fga + fta) > 0 else 0

        return {
            "Points": total_pts,
            "FG%": f"{fg_pct:.1%}",
            "3P%": f"{three_pct:.1%}",
            "FT%": f"{ft_pct:.1%}",
            "TS%": f"{ts_pct:.1%}",
            "FGA": fga,
            "FTA": fta,
        }

    def get_clutch_data(
        self, player_id: int = None, player_name: str = None
    ) -> pd.DataFrame | None:
        """筛选出目标球员关键时刻的数据

        Args:
            player_id: 球员 ID
            player_name: 球员名字

        Returns:
            关键时刻的数据表格
        """
        query = """
            -- 提取球员在关键时刻的所有动作片段
            SELECT
                personid,
                gameid,
                actionnumber,
                clock,
                period,
                playername, -- 球员名称
                shotdistance, -- 投篮距离
                shotresult,
                isfieldgoal,
                scorehome,
                scoreaway,
                pointstotal,
                actiontype,
                subtype,
                shotvalue,
                description
            FROM game_pbp
            WHERE period >= 4
            ORDER BY gameid, period, actionnumber;
        """

        df = self.db.query(query)

        # 清洗并筛选关键时刻数据
        df = self.process_clutch_df(df)

        # 过滤出目标球员的动作
        if player_id:
            return df[df["personId"] == player_id]
        if player_name:
            return df[df["playerName"] == player_name]
        else:
            print("Please provide a player ID or name!")
            return None

    def compare_players(self, player_ids):
        """横向对比多名球员"""
        pass

    def process_clutch_df(self, df: pd.DataFrame) -> pd.DataFrame:
        """清洗并筛选关键时刻数据

        Args:
            df: 需要处理的表格

        Returns:
            筛选后的表格数据
        """
        df["seconds_remaining"] = df["clock"].apply(parse_v3_clock)

        df["home_pts"] = pd.to_numeric(df["scoreHome"], errors="coerce")
        df["away_pts"] = pd.to_numeric(df["scoreAway"], errors="coerce")
        df["margin"] = (df["home_pts"] - df["away_pts"]).abs()

        # 关键时刻过滤条件
        clutch_mask = (
            (df["period"] >= 4) & (df["seconds_remaining"] <= 300) & (df["margin"] <= 5)
        )

        return df[clutch_mask].copy()
