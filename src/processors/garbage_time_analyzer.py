"""垃圾时间数据分析"""

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


class GarbageTimeAnalyzer(BaseAnalyzer):
    def __init__(self, db_manager):
        super().__init__(db_manager)
        self._df = pd.DataFrame()  # 垃圾时间总表格

    def analyze_player(self, player_id: int = None, player_name: str = None) -> Dict:
        """核心出口：输入球员信息，直接返回该球员的垃圾时间分析报告

        Args:
            player_id: 球员 ID
            player_name: 球员名称

        Returns:
            球员垃圾时间数据报告
        """
        # 每次开始前重置为空表格, 防止上次搜索的结果残留
        self._df = pd.DataFrame()

        # 获取数据
        self._get_garbage_time_data(player_id=player_id, player_name=player_name)

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
        """计算垃圾时间的指标

        Returns:
            垃圾时间的相关指标
        """
        if self._df.empty:
            return {}

        # 投篮统计
        fga_df = self._df[self._df["isFieldGoal"] == 1]
        # 三分统计
        three_pa_df = fga_df[fga_df["shotValue"] == 3]
        # 罚球统计
        fta_df = self._df[
            self._df["actionType"].str.contains("Free Throw", case=False, na=False)
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

    def _get_garbage_time_data(self, player_id: int = None, player_name: str = None):
        """筛选出目标球员垃圾时间的数据

        Args:
            player_id: 球员 ID
            player_name: 球员名字
        """
        query = """
            -- 提取球员在垃圾时间的所有动作片段
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

        # 清洗并筛选垃圾时间数据
        self._process_garbage_df()

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

    def _process_garbage_df(self):
        """清洗并筛选垃圾时间数据"""
        self._df["seconds_remaining"] = self._df["clock"].apply(parse_v3_clock)

        self._df["home_pts"] = pd.to_numeric(self._df["scoreHome"], errors="coerce")
        self._df["away_pts"] = pd.to_numeric(self._df["scoreAway"], errors="coerce")

        self._df["margin"] = (self._df["home_pts"] - self._df["away_pts"]).abs()

        # 垃圾时间过滤条件
        self._df = self._df[
            (self._df["period"] >= 4)
            & (self._df["seconds_remaining"] <= 300)
            & (self._df["margin"] > 15)
        ]
