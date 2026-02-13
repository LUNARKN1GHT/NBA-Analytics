import logging
from pathlib import Path

# --- 核心路径管理 ---
BASE_DIR = Path(__file__).resolve().parent
DATA_RAW = BASE_DIR / "data" / "raw"
DATA_PROCESSED = BASE_DIR / "data" / "processed"
REPORTS_DIR = BASE_DIR / "reports"
LOGS_DIR = BASE_DIR / "logs"

DB_PATH = DATA_RAW / "nba.sqlite"

# --- 任务参数配置 ---
SUB_TASKS = ["duration", "home_advantage", "player_stats", "clutch"]

# 生成赛季列表
START_YEAR = 1985
END_YEAR = 2024
SEASONS = [f"{y}-{str(y+1)[-2:]}" for y in range(START_YEAR, END_YEAR + 1)]


# --- 初始化工具 ---
def init_project_structure():
    """初始化必要的文件夹结构"""
    dirs = [DATA_RAW, DATA_PROCESSED, REPORTS_DIR, LOGS_DIR]
    for task in SUB_TASKS:
        dirs.append(DATA_PROCESSED / task)
        dirs.append(REPORTS_DIR / task)

    for d in dirs:
        d.mkdir(parents=True, exist_ok=True)


def setup_logger(name="nba_analytics"):
    """全局日志配置"""
    # 创建logs目录
    LOGS_DIR.mkdir(exist_ok=True)

    task_logger = logging.getLogger(name)
    task_logger.setLevel(logging.INFO)
    task_logger.handlers.clear()

    # 配置日志格式
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )

    # 处理器配置: (级别, 文件名, 是否权限错误)
    handlers = [
        (logging.WARNING, None, False),  # 控制台
        (logging.INFO, LOGS_DIR / f"{name}.log", False),  # 全量文件
        (logging.ERROR, LOGS_DIR / f"{name}_error.log", True),  # 仅错误文件
    ]

    for level, path, error_only in handlers:
        handler = (
            logging.FileHandler(path, encoding="utf-8")
            if path
            else logging.StreamHandler()
        )
        handler.setLevel(level)
        handler.setFormatter(formatter)
        task_logger.addHandler(handler)

    return task_logger


# --- 自动执行 ---
init_project_structure()
logger = setup_logger()
