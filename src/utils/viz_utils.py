import os
from typing import Callable, Dict

import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns

import config


class NBAVisualizer:
    def __init__(self):
        # 基础样式设定
        sns.set_theme(style="whitegrid")
        plt.rcParams["font.sans-serif"] = ["SimHei"]  # 解决中文显示
        plt.rcParams["axes.unicode_minus"] = False  # 解决负号显示

        # 任务类型与保存子目录的映射关系
        self.task_map = {
            "duration": "game_analysis",
            "home_advantage_trend": "game_analysis",
            "three_attempt_trend": "game_analysis",
            "clutch_efficiency": "player_clutch",
            "decision_matrix": "player_clutch",
            "team_efficiency": "team_analysis",
            "score_margin_dist": "player_analysis",
        }

        self._plotters: Dict[str, Callable[[pd.DataFrame], None]] = {
            "duration": self._plot_duration,
            "home_advantage_trend": self._plot_home_advantage_trend,
            "three_attempt_trend": self._plot_three_attempt_trend,
            "score_margin_dist": self._plot_score_margin_dist,
        }

    def _save_logic(self, task_type: str, fig_name: str) -> None:
        """内部通用保存逻辑"""
        sub_dir = self.task_map.get(task_type, "misc")
        save_path = os.path.join(config.REPORTS_DIR, sub_dir)
        os.makedirs(save_path, exist_ok=True)

        full_path = os.path.join(save_path, f"{fig_name}.png")
        plt.savefig(full_path, dpi=300, bbox_inches="tight")
        plt.close()
        print(f"📈 绘图完成，保存至: {full_path}")

    def plot(self, task_type, df, **kwargs):
        if df.empty:
            print(f"⚠️ {task_type} 数据为空，跳过绘图。")
            return

        p_name = kwargs.get("player_name") or df.attrs.get("player_name", "Unknown")
        p_id = kwargs.get("player_id") or df.attrs.get("player_id", "")

        # 构建一个唯一的文件名标识
        fig_identity = f"{p_name}_{p_id}".replace(" ", "_").strip("_")
        fig_name = f"{fig_identity}_{task_type}"

        plotter = self._plotters.get(task_type)
        plotter(df, player_name=p_name, player_id=p_id)

        if plotter is None:
            raise ValueError(f"未支持的绘图任务：{task_type}")

        plotter(df)
        self._save_logic(task_type, fig_name)

    # --- 私有绘图函数 ---

    def _plot_duration(self, df, **kwargs):
        """具体的时长趋势绘图逻辑"""
        plt.figure(figsize=(12, 6))
        sns.lineplot(
            data=df,
            x="season_year",
            y="avg_duration",
            marker="o",
            color="#17408B",
            linewidth=2.5,
        )

        # 添加阴影区间 (±5%) 增加专业感
        plt.fill_between(
            df["season_year"],
            df["avg_duration"] * 0.98,
            df["avg_duration"] * 1.02,
            alpha=0.1,
            color="#17408B",
        )

        # 年滚动平均
        sns.lineplot(
            data=df,
            x="season_year",
            y="duration_ma3",
            linewidth=3,
            color="#C9082A",
            label="3 年滚动平均",
        )

        # LOWESS 平滑趋势线
        sns.regplot(
            data=df,
            x="season_year",
            y="avg_duration",
            lowess=True,
            scatter=False,
            color="black",
            line_kws={"linewidth": 2, "linestyle": "--", "label": "平滑趋势"},
        )

        plt.title(kwargs.get("title", "NBA 比赛平均时长演变趋势"), fontsize=16)
        plt.xlabel("年份", fontsize=12)
        plt.ylabel("时长 (分钟)", fontsize=12)
        plt.xticks(rotation=45)
        plt.legend()
        plt.tight_layout()

    def _plot_home_advantage_trend(self, df, **kwargs):
        """具体的主场优势趋势绘图逻辑"""
        # 按赛季计算全联盟平均主场优势
        trend = df.groupby("season_id")["ha_diff"].mean().reset_index()

        # 添加滑动平均列
        trend["ha_diff_ma3"] = trend["ha_diff"].rolling(window=3, min_periods=3).mean()

        plt.figure(figsize=(12, 6))

        # 主要趋势线
        plt.plot(
            trend["season_id"].astype(str),
            trend["ha_diff"],
            marker="o",
            linestyle="-",
            color="orange",
            linewidth=2.5,
            label="原始数据",
            alpha=0.7,
        )

        # 添加阴影区间 (±2%) 增加专业感
        plt.fill_between(
            range(len(trend)),
            trend["ha_diff"] * 0.98,
            trend["ha_diff"] * 1.02,
            alpha=0.1,
            color="orange",
        )

        # 3年滑动平均线
        plt.plot(
            trend["season_id"].astype(str),
            trend["ha_diff_ma3"],
            linewidth=3,
            color="#C9082A",
            label="3年滑动平均",
        )

        plt.axhline(0, color="black", linewidth=1)
        plt.xticks(
            range(len(trend)), trend["season_id"].astype(str), rotation=45, fontsize=10
        )
        plt.title(kwargs.get("title", "NBA 联盟主场优势趋势分析"), fontsize=16)
        plt.xlabel("赛季", fontsize=12)
        plt.ylabel("胜率差异 (主场 - 客场 %)", fontsize=12)
        plt.grid(True, alpha=0.3)
        plt.legend()
        plt.tight_layout()

    def _plot_three_attempt_trend(self, df, **kwargs):
        """绘制三分占比变化"""
        plt.figure(figsize=(12, 6))

        # 主要趋势线
        plt.plot(
            df["season"],
            df["fg3a_pct"],
            marker="o",
            linestyle="-",
            color="orange",
            linewidth=2.5,
            label="三分出手比例",
            alpha=0.7,
        )

        plt.title(kwargs.get("title", "三分出手占比演变趋势"), fontsize=16)
        plt.xlabel("年份", fontsize=12)
        plt.ylabel("出手占比", fontsize=12)
        plt.xticks(rotation=45)
        plt.legend()
        plt.tight_layout()

    def _plot_score_margin_dist(self, df, **kwargs):
        """绘制球员得分随分差分布的曲线图"""
        player_name = kwargs.get("player_name", "Unknown Player")

        plt.figure(figsize=(14, 7))

        # 使用 核密度估计 或 平滑线 来展示趋势，原始点作为辅助
        # 1. 绘制得分柱状图
        sns.barplot(
            data=df,
            x="margin",
            y="total_points",
            color="skyblue",
            alpha=0.4,
            label="单分差的得分",
        )

        # 2. 绘制移动平均线或平滑趋势线
        # 由于分差可能不连续，我们先确保排序
        df = df.sort_values("margin")
        df["points_smooth"] = df["total_points"].rolling(window=3, center=True).mean()

        sns.lineplot(
            data=df,
            x=df.index,
            y="points_smooth",
            color="#C9082A",
            linewidth=2.5,
            label="得分趋势（MA3）",
        )

        # 3. 辅助线：分差为 0 的轴
        if 0 in df["margin"].values:
            zero_idx = df.index.get_loc(df[df["margin"] == 0].index[0])
            plt.axvline(zero_idx, color="black", linestyle="--", alpha=0.5)

        plt.title(f"得分分布随比赛分差波动图: {player_name}", fontsize=18)
        plt.xlabel("分差 (Margin)", fontsize=12)
        plt.ylabel("累积得分", fontsize=12)

        # 优化 X 轴标签，每隔 5 个显示一个
        locs, labels = plt.xticks()
        plt.xticks(locs[::5], df["margin"].iloc[::5], rotation=0)

        plt.legend(loc="upper left")
        plt.tight_layout()
