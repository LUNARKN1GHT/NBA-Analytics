"""得分与球队分差的分布分析"""

import os

import pandas as pd

from config import DATA_PROCESSED
from src.processors.base_analyzer import BaseAnalyzer


class ScoreMarginAnalyzer(BaseAnalyzer):
    def __init__(self, db_manager):
        super().__init__(db_manager)
        self._pbp_df = pd.DataFrame()
        self._stats = pd.DataFrame()

    def analyze_player(
        self, player_id: int = None, player_name: str = None
    ) -> pd.DataFrame | None:
        # 重置状态
        self._pbp_df = pd.DataFrame()
        self._stats = pd.DataFrame()

        # 获取数据
        self._get_pbp_data(player_id=player_id, player_name=player_name)

        if self._pbp_df is None or self._pbp_df.empty:
            name = player_name if player_name else player_id
            print(f"--- [Warning] No data found for {name} ---")
            return None

        # 处理数据
        self._process_pbp_df()

        # 聚合统计
        self._analyze_margin_distribution()

        # 执行保存逻辑
        # 这里尝试从数据中获取确切的名字或 ID
        final_name = (
            player_name if player_name else str(self._pbp_df["playerName"].iloc[0])
        )
        final_id = player_id if player_id else int(self._pbp_df["personId"].iloc[0])

        self._save_stat(player_name=final_name, player_id=final_id)

        self._stats.attrs["player_name"] = (
            player_name if player_name else self._pbp_df["playerName"].iloc[0]
        )
        self._stats.attrs["player_id"] = (
            player_id if player_id else self._pbp_df["personId"].iloc[0]
        )

        return self._stats

    def _save_stat(self, player_id: int = None, player_name: str = None):
        if self._stats is None or self._stats.empty:
            return

        # 规范化文件夹名称（将空格转为下划线）
        safe_name = player_name.replace(" ", "_")
        folder_id = str(player_id) if player_id else "unknown"

        # 构建路径: data/processed/score@margin/LeBron_James_2544/
        save_dir = DATA_PROCESSED / "score@margin" / f"{safe_name}_{folder_id}"
        os.makedirs(save_dir, exist_ok=True)

        # 保存文件
        file_path = save_dir / "raw_margin_stats.csv"
        self._stats.to_csv(file_path, index=False, encoding="utf-8-sig")

        print(f"✅ 统计报告已保存至: {file_path}")

    def _get_pbp_data(self, player_id: int = None, player_name: str = None):
        """得到球员在比赛中的数据

        Args:
            player_id: 球员 ID
            player_name: 球员名字
        """
        query = """
                -- 提取球员在比赛中的所有动作片段
                SELECT
                    personid,
                    gameid,
                    actionnumber,
                    -- 我们只考虑分差，不用考虑时间
                    period,
                    playername,   -- 球员名称
                    shotdistance, -- 投篮距离
                    shotresult,
                    isfieldgoal,
                    scorehome,
                    scoreaway,
                    location, -- 进球方
                    pointstotal,
                    actiontype,
                    subtype,
                    shotvalue,
                    description
                FROM
                    game_pbp
                ORDER BY
                    gameid,
                    period,
                    actionnumber;
                """

        self._pbp_df = self.db.query(query)

        if self._pbp_df.empty:
            self._pbp_df = pd.DataFrame()
            return

        # 过滤出目标球员的动作
        if player_id:
            self._pbp_df = self._pbp_df[self._pbp_df["personId"] == player_id]
        elif player_name:
            self._pbp_df = self._pbp_df[self._pbp_df["playerName"] == player_name]
        else:
            print("Please provide a player ID or name!")
            self._pbp_df = None

    def _analyze_margin_distribution(self):
        """将投篮数据按分差区间聚合统计"""
        if self._pbp_df is None or self._pbp_df.empty:
            self._stats = None

        # 执行聚合
        self._stats = (
            self._pbp_df.groupby("margin")
            .agg(
                attempts=("isFieldGoal", "count"),  # 总出手数
                made=("is_made", "sum"),  # 命中数
                total_points=("pts_scored", "sum"),  # 该分差下的总得分
                avd_distance=("shotDistance", "mean"),  # 该分差下的平均投篮距离
            )
            .reset_index()
        )

        # 计算派生指标
        self._stats["fg_pct"] = (self._stats["made"] / self._stats["attempts"]).map(
            "{:.1%}".format
        )
        # 计算得分占比
        self._stats["pts_dist"] = (
            self._stats["total_points"] / self._stats["total_points"].sum()
        ).map("{:.1%}".format)

    def _process_pbp_df(self):
        """清洗、整理比赛数据"""
        if self._pbp_df.empty:
            return

        # 转换数值类型确保计算不出错
        for col in ["scoreHome", "scoreAway", "shotValue"]:
            self._pbp_df[col] = pd.to_numeric(self._pbp_df[col], errors="coerce")

        # 计算出手瞬间该球员所在球队的领先/落后分数
        # 这里根据 location 计算分差
        self._pbp_df["margin"] = 0

        home_mask = self._pbp_df["location"] == "h"
        away_mask = self._pbp_df["location"] == "v"

        self._pbp_df.loc[home_mask, "margin"] = (
            self._pbp_df["scoreHome"]
            - self._pbp_df["scoreAway"]
            - self._pbp_df["shotValue"]
        )
        self._pbp_df.loc[away_mask, "margin"] = (
            self._pbp_df["scoreAway"]
            - self._pbp_df["scoreHome"]
            - self._pbp_df["shotValue"]
        )

        # 标记是否命中
        self._pbp_df["is_made"] = (
            self._pbp_df["shotResult"].str.contains("Made", case=False).astype(int)
        )
        # 计算该动作得分
        self._pbp_df["pts_scored"] = self._pbp_df["is_made"] * self._pbp_df["shotValue"]

    def compare_players(self, player_ids):
        """横向对比多名球员"""
        pass
