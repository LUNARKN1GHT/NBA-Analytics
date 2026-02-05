import os

import matplotlib.pyplot as plt
import seaborn as sns

import config


class NBAVisualizer:
    def __init__(self):
        sns.set_theme(style="whitegrid")
        plt.rcParams["font.sans-serif"] = ["SimHei"]
        plt.rcParams["axes.unicode_minus"] = False

    def save_plot(self, task_name, fig_name):
        """通用保存逻辑：定位到 reports/{task_name}/{fig_name}"""
        save_path = os.path.join(config.REPORTS_DIR, task_name)
        os.makedirs(save_path, exist_ok=True)
        path = os.path.join(config.REPORTS_DIR, task_name, fig_name)
        plt.savefig(path)
        plt.close()
        print(f"📈 图表已保存至: {path}")

    def plot_duration_trend(self, df):
        """时长趋势绘图"""
        plt.figure(figsize=(12, 6))
        sns.lineplot(data=df, x="season_year", y="avg_duration", marker="o")

        # 填充最大/最小值的区间
        plt.fill_between(
            df["season_year"],
            df["avg_duration"] * 0.95,
            df["avg_duration"] * 1.05,
            alpha=0.1,
            color="#17408B",
        )

        plt.title("NBA 常规赛平均比赛时长演变趋势", fontsize=16, pad=20)
        plt.xlabel("赛季", fontsize=12)
        plt.ylabel("平均时长 (分钟)", fontsize=12)
        plt.xticks(rotation=45)

        # 调用类内部的保存逻辑，指定子任务文件夹
        plt.tight_layout()
        self.save_plot("duration", "trend_chart.png")

    def plot_home_advantage(self, df):
        """主场优势胜率"""
        plt.figure(figsize=(12, 6))
        sns.lineplot(data=df, x="season_year", y="home_win_pct", marker="o")

        plt.title("NBA 常规赛主场优势演变趋势", fontsize=16, pad=20)
        plt.xlabel("赛季", fontsize=12)
        plt.ylabel("主场优势胜利优势 (百分比)", fontsize=12)
        plt.xticks(rotation=45)

        plt.tight_layout()
        self.save_plot("home_advantage", "trend_chart.png")

    def plot_clutch_performance(self, df):
        if df.empty:
            print("⚠️ DataFrame 为空，取消绘图。")
            return

        # 只在绘图层派生，不影响统计
        df = df.copy()
        df["isolator_rate"] = df["isolator_made"] / df["clutch_made"]

        plt.figure(figsize=(28, 21))

        scatter = plt.scatter(
            df["clutch_attempts"],
            df["clutch_fg_pct"],
            c=df["isolator_rate"],
            cmap="YlOrRd",
            s=60,
            alpha=0.75,
            edgecolors="w",
            linewidth=0.4,
        )

        # plt.xscale("log")

        # y 轴仍然动态
        y_min = max(0, df["clutch_fg_pct"].min() - 0.05)
        y_max = min(1, df["clutch_fg_pct"].max() + 0.05)
        plt.ylim(y_min, y_max)

        cbar = plt.colorbar(scatter)
        cbar.set_label("硬解率（硬解命中 / 总命中）", rotation=270, labelpad=20)

        # 中位数辅助线（在 log 轴上依然有意义）
        plt.axhline(
            df["clutch_fg_pct"].median(), color="gray", linestyle="--", alpha=0.4
        )
        plt.axvline(
            df["clutch_attempts"].median(), color="gray", linestyle="--", alpha=0.4
        )

        # 标注产量前考前的球员
        top_players = df.nlargest(20, "clutch_attempts")
        for _, row in top_players.iterrows():
            plt.text(
                row["clutch_attempts"] * 1.01,  # log 轴下用比例偏移
                row["clutch_fg_pct"],
                row["player_name"],
                fontsize=8,
                va="center",
            )

        plt.title("NBA 关键时刻：产量、效率与硬解能力分布", fontsize=15, pad=20)
        plt.xlabel("关键时刻投篮出手次数（对数刻度）")
        plt.ylabel("关键时刻命中率 (Clutch FG%)")

        plt.tight_layout()
        self.save_plot("clutch", "clutch_pro_analysis.png")
