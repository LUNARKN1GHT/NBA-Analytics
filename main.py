from src.processors.NBA_analyzer import NBAAnalyzer
from src.utils.viz_utils import NBAVisualizer


def main():
    nba = NBAAnalyzer("data/raw/nba.sqlite")

    print("📊 正在执行：比赛时长趋势分析...")
    duration_df = nba.analyze_duration()
    viz = NBAVisualizer()
    viz.plot("duration", duration_df, title="联盟历年时长演变")

    nba.disconnect()


if __name__ == "__main__":
    main()
