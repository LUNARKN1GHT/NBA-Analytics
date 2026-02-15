# main.py
from src.data.load_data import NBALoader


def main():
    # 实例化加载器
    loader = NBALoader()

    print(f"当前数据库配置路径为: {loader.db_path}")

    # 下载历史选秀数据
    loader.fetch_draft_history()

    # loader.fetch_player_career(player_ids=PLAYERS)

    # 获取所有球员信息
    # loader.fetch_all_players()

    # 获取某一年的比赛数据
    # loader.fetch_games(seasons=seasons)

    # 获取球员比赛记录
    # loader.fetch_player_game_logs(player_ids=PLAYERS, seasons=SEASONS)

    # 获取所有比赛的 pbp 数据

    # 获取比赛 PBP数据
    # game_ids = loader.get_local_player_game_ids(player_id=2544)
    # loader.fetch_pbp_data(game_ids=game_ids)


if __name__ == "__main__":
    main()
