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
