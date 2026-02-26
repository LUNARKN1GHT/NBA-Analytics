from src.processors.NBA_analyzer import NBAAnalyzer
from src.utils.viz_utils import NBAVisualizer


def show_menu():
    """显示交互式菜单"""
    print("\n" + "=" * 50)
    print("🏀 NBA数据分析工具")
    print("=" * 50)
    print("1. 比赛时长趋势分析")
    print("2. 主场优势分析")
    print("3. 三分出手趋势分析")
    print("4. 球员关键时刻分析")
    print("5. 球员垃圾时间分析")
    print("6. 球员得分分布分析")
    print("6. 批量分析多个球员")
    print("0. 退出程序")
    print("=" * 50)


def get_player_input():
    """获取球员输入"""
    print("\n请选择球员输入方式:")
    print("1. 输入球员ID")
    print("2. 输入球员姓名")

    choice = input("请选择 (1/2): ").strip()

    if choice == "1":
        player_id = input("请输入球员ID: ").strip()
        return {"player_id": int(player_id) if player_id.isdigit() else None}
    elif choice == "2":
        player_name = input("请输入球员姓名: ").strip()
        return {"player_name": player_name if player_name else None}
    else:
        print("无效选择，使用默认球员")
        return {"player_id": 2544}


def batch_player_analysis(nba, viz):
    """批量分析多个球员"""
    print("\n批量球员分析模式")
    player_ids = input("请输入球员ID列表(用逗号分隔): ").strip()

    if player_ids:
        ids = [
            int(pid.strip()) for pid in player_ids.split(",") if pid.strip().isdigit()
        ]
        print(f"\n将分析 {len(ids)} 位球员的关键时刻表现:")

        for player_id in ids:
            try:
                print(f"\n--- 分析球员ID: {player_id} ---")
                result = nba.analyze_player_clutch(player_id=player_id)
                if result:
                    print(f"球员: {result.get('Player', '未知')}")
                    print(f"比赛场次: {result.get('Game_Count', 0)}")
                    print(f"总得分: {result.get('Points', 0)}")
                    print(f"投篮命中率: {result.get('FG%', 'N/A')}")
                else:
                    print("未找到该球员数据")
            except Exception as e:
                print(f"分析球员 {player_id} 时出错: {e}")


def main():
    nba = NBAAnalyzer("data/raw/nba.sqlite")
    viz = NBAVisualizer()

    try:
        while True:
            show_menu()
            choice = input("请选择功能 (0-6): ").strip()

            if choice == "0":
                print("👋 感谢使用，再见！")
                break

            elif choice == "1":
                print("📊 正在执行：比赛时长趋势分析...")
                duration_df = nba.analyze_duration()
                viz.plot("duration", duration_df, title="联盟历年时长演变")

            elif choice == "2":
                print("📊 正在执行：主场优势分析...")
                home_advantage = nba.analyze_home_advantage()
                viz.plot("home_advantage_trend", home_advantage, title="主场优势分析")

            elif choice == "3":
                print("📊 正在执行：三分出手趋势...")
                three_attempt_df = nba.analyze_three_attempt_trend()
                viz.plot("three_attempt_trend", three_attempt_df, title="三分出手比例")

            elif choice == "4":
                player_info = get_player_input()
                print("📊 正在执行：生涯关键时刻统计...")
                clutch_performance = nba.analyze_player_clutch(**player_info)
                print(clutch_performance)

            elif choice == "5":
                player_info = get_player_input()
                print("📊 正在执行：垃圾时间统计...")
                garbage_performance = nba.analyze_player_garbage_time(**player_info)
                print(garbage_performance)

            elif choice == "6":
                player_info = get_player_input()
                print("📊 正在执行：球员得分分布分析...")
                score_margin_distribution = nba.analyze_score_margin(**player_info)

                if score_margin_distribution is not None:
                    viz.plot(
                        task_type="score_margin_dist",
                        df=score_margin_distribution,
                        player_name=player_info.get("player_name"),
                        player_id=player_info.get("player_id"),
                    )

            elif choice == "7":
                batch_player_analysis(nba, viz)

            else:
                print("❌ 无效选择，请重新输入")

            # 询问是否继续
            if choice != "0":
                continue_choice = input("\n是否继续分析？(y/n): ").strip().lower()
                if continue_choice != "y":
                    break

    except KeyboardInterrupt:
        print("\n\n👋 程序被用户中断")
    except Exception as e:
        print(f"\n❌ 程序执行出错: {e}")
    finally:
        nba.disconnect()


if __name__ == "__main__":
    main()
