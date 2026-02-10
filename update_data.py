# main.py

from src.data.load_data import NBALoader


def main():
    # 实例化加载器
    loader = NBALoader()

    print(f"当前数据库配置路径为: {loader.db_path}")


if __name__ == "__main__":
    main()
