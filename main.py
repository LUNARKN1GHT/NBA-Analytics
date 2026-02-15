from src.processors.NBA_analyzer import NBAAnalyzer


def main():
    nba = NBAAnalyzer("data/raw/nba.sqlite")

    # print("📊 正在执行：比赛时长趋势分析...")
    # duration_df = nba.analyze_duration()
    # viz = NBAVisualizer()
    # viz.plot("duration", duration_df, title="联盟历年时长演变")

    # print("📊 正在执行：主场优势分析...")
    # home_advantage = nba.analyze_home_advantage()
    # viz = NBAVisualizer()
    # viz.plot("home_advantage_trend", home_advantage, title="主场优势分析")

    # print("📊 正在执行：三分出手趋势...")
    # three_attempt_df = nba.analyze_three_attempt_trend()
    # viz = NBAVisualizer()
    # viz.plot("three_attempt_trend", three_attempt_df, title="三分出手比例")

    print("📊 正在执行：生涯关键时刻统计...")
    clutch_performance = nba.analyze_player_clutch(player_id=201939)
    print(clutch_performance)

    nba.disconnect()


if __name__ == "__main__":
    main()
