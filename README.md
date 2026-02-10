# NBA 数据分析

## 数据

原始数据来自官方 [NBA Stats](https://stats.nba.com/)。数据通过 `nba_api`
包进行下载，本项目设计了单独的类 [load_data.py](src/data/load_data.py) 进行原始数据下载与管理。

接口函数汇总参考 [API_doc](/doc/API.md)。