import os

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# 赛季列表
seasons = [
    "1985-86",
    "1986-87",
    "1987-88",
    "1988-89",
    "1989-90",
    "1990-91",
    "1991-92",
    "1992-93",
    "1993-94",
    "1994-95",
    "1995-96",
    "1996-97",
    "1997-98",
    "1998-99",
    "1999-00",
    "2000-01",
    "2001-02",
    "2002-03",
    "2003-04",
    "2004-05",
    "2005-06",
    "2006-07",
    "2007-08",
    "2008-09",
    "2009-10",
    "2010-11",
    "2011-12",
    "2012-13",
    "2013-14",
    "2014-15",
    "2015-16",
    "2016-17",
    "2017-18",
    "2018-19",
    "2019-20",
    "2020-21",
    "2021-22",
    "2022-23",
    "2023-24",
    "2024-25",
]

# 基础路径
DATA_RAW = os.path.join(BASE_DIR, "data", "raw")
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
