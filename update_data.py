# main.py

from src.data.load_data import NBALoader


def main():
    # 实例化加载器
    loader = NBALoader()

    print(f"当前数据库配置路径为: {loader.db_path}")

    # 示例 ID：勒布朗 (2544), 库里 (201939), 杜兰特 (201142)
    test_ids = [2544, 201939, 201142]

    loader.fetch_player_career(player_ids=test_ids)

    # 获取所有球员信息
    # loader.fetch_all_players()


if __name__ == "__main__":
    main()
