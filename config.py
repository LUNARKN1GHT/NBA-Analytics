import os

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# 基础路径
DATA_RAW = os.path.join(BASE_DIR, "data", "raw")
DB_PATH = os.path.join(BASE_DIR, "data", "raw", "nba.sqlite")
DATA_PROCESSED = os.path.join(BASE_DIR, "data", "processed")
REPORTS_DIR = os.path.join(BASE_DIR, "reports")

# 定义子任务文件夹 (按需扩展)
# 例如：duration(时长), clutch(关键时刻), shooting(投篮)
SUB_TASKS = ["duration", "home_advantage", "player_stats"]

# 数据库路径
DB_PATH = os.path.join(DATA_RAW, "nba.sqlite")


def init_project_structure():
    """初始化文件夹结构"""
    for task in SUB_TASKS:
        os.makedirs(os.path.join(DATA_PROCESSED, task), exist_ok=True)
        os.makedirs(os.path.join(REPORTS_DIR, task), exist_ok=True)


# 立即初始化
init_project_structure()
