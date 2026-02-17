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
    def __init__(self, db_manager):
        super().__init__(db_manager)
        self._df = pd.DataFrame()  # 关键时刻总表格

    def analyze_player(self, player_id: int = None, player_name: str = None) -> Dict:
        """核心出口：输入球员信息，直接返回该球员的关键时刻分析报告

        Args:
            player_id: 球员 ID
            player_name: 球员名称

        Returns:
            球员关键时刻数据报告
        """
        # 每次开始前重置为空表格, 防止上次搜索的结果残留
        self._df = pd.DataFrame()

        # 获取数据
        self._get_clutch_data(player_id=player_id, player_name=player_name)

        # 如果 _get_clutch_data 之后依然是空的，直接返回
        if self._df is None or self._df.empty:
            name = player_name if player_name else player_id
            print(f"--- [Warning] No data found for {name} ---")
            return {}
        metrics = self.calculate_metrics()

        metrics["Player"] = (
            player_name if player_name else self._df["playerName"].iloc[0]
        )
        metrics["Game_Count"] = self._df["gameId"].nunique()

        return metrics

    def calculate_metrics(self) -> Dict:
        """计算关键时刻的指标

        Returns:
            关键时刻的相关指标
        """
        if self._df.empty:
            return {}

        metrics = self._calculate_clutch_shot_metrics()

        metrics["shoot_distance"] = self._calculate_clutch_shot_distance_metrics()

        return metrics

    def _calculate_clutch_shot_metrics(self) -> Dict:
        """计算关键时刻的投篮指标"""

        self._df["isFieldGoal"] = pd.to_numeric(
            self._df["isFieldGoal"], errors="coerce"
        )

        # 投篮统计
        fga_df = self._df[self._df["isFieldGoal"] == 1]
        fgm = len(fga_df[fga_df["shotResult"] == "Made"])

        # 三分统计
        three_pa_df = fga_df[fga_df["shotValue"] == 3]
        three_pm = len(three_pa_df[three_pa_df["shotResult"] == "Made"])

        # 罚球统计
        fta_df = self._df[
            self._df["actionType"].str.contains("Free Throw", case=False, na=False)
        ]
        ftm = len(
            fta_df[~fta_df["description"].str.contains("MISS", case=False, na=False)]
        )

        # 计算投篮数
        fga = len(fga_df)
        three_pa = len(three_pa_df)
        fta = len(fta_df)

        # 总得分统计
        total_pts = (fgm - three_pm) * 2 + three_pm * 3 + ftm

        # 真实命中率
        ts_pct = total_pts / (2 * (fga + 0.44 * fta)) if (fga + fta) > 0 else 0

        # 三分出手占比
        three_rate = (three_pa / fga) if fga > 0 else 0

        return {
            "Points": total_pts,
            "FG%": f"{(fgm/fga):.1%}",
            "3P%": f"{(three_pm/three_pa):.1%}",
            "FT%": f"{(ftm/fta):.1%}",
            "TS%": f"{ts_pct:.1%}",
            "3P_Freq": f"{three_rate:.1%}",
            "FGA": fga,
            "FTA": fta,
        }

    def _get_clutch_data(self, player_id: int = None, player_name: str = None):
        """筛选出目标球员关键时刻的数据

        Args:
            player_id: 球员 ID
            player_name: 球员名字
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

        self._df = self.db.query(query)

        if self._df.empty:
            self._df = pd.DataFrame()
            return

        # 清洗并筛选关键时刻数据
        self._process_clutch_df()

        # 过滤出目标球员的动作
        if player_id:
            self._df = self._df[self._df["personId"] == player_id]
        elif player_name:
            self._df = self._df[self._df["playerName"] == player_name]
        else:
            print("Please provide a player ID or name!")
            self._df = None

    def compare_players(self, player_ids):
        """横向对比多名球员"""
        pass

    def _process_clutch_df(self):
        """清洗并筛选关键时刻数据"""
        self._df["seconds_remaining"] = self._df["clock"].apply(parse_v3_clock)

        self._df["home_pts"] = pd.to_numeric(self._df["scoreHome"], errors="coerce")
        self._df["away_pts"] = pd.to_numeric(self._df["scoreAway"], errors="coerce")

        self._df["margin"] = (self._df["home_pts"] - self._df["away_pts"]).abs()

        # 关键时刻过滤条件
        self._df = self._df[
            (self._df["period"] >= 4)
            & (self._df["seconds_remaining"] <= 300)
            & (self._df["margin"] <= 5)
        ]
