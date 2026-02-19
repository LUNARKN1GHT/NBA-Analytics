# NBA Analytics Platform

一个基于Python的NBA数据分析平台，提供全面的球员表现分析、比赛趋势洞察和关键数据可视化功能。

## 项目概述

本项目旨在通过分析NBA官方数据，深入挖掘篮球比赛中的各种统计模式和趋势。平台支持多种分析维度，包括：

- 球员关键时表现分析
- 比赛市场和节奏变化趋势
- 主场优势效应研究
- 垃圾时间数据分析
- 球队和球员综合统计
- （后续更新）

目标：从数据角度理解比赛表现与决策模式。

## 技术栈

- Python
- SQLite
- pandas / numpy
- matplotlib
- NBA 官方数据 (`nba_api`)

## 快速开始

### 环境要求

- Python 3.8+
- SQLite3
- 相关依赖包

## 项目结构

```text
NBA-Analytics/
├── main.py              # 程序入口
├── update_data.py       # 数据更新脚本
├── src/
│   ├── data/            # 数据下载与管理
│   ├── processors/      # 分析模块
│   └── utils/           # 数据库与可视化工具
├── data/                # 原始 & 处理后数据
├── reports/             # 生成的图表与报告
└── doc/                 # 说明文档
```

## 核心功能

### 数据管理

- **自动数据下载**: 通过`nba_api`包从官方NBA Stats获取数据
- **增量更新**: 智能识别已存在数据，避免重复下载
- **SQLite存储**: 结构化存储球员、比赛和事件数据

### 分析模块

分析模块支持扩展，可根据需要增加新的分析模块。

- 关键时刻分析 (`clutch_analyzer.py`)
- 比赛趋势分析 (`game_analyzer.py`)
- 垃圾时间分析 (`garbage_time_analyzer.py`)

### 可视化功能 (`viz_utils.py`)

- 自动生成趋势图表
- 支持多种图表类型（折线图、柱状图等）
- 中文标签和专业样式

## 数据

原始数据来自官方 [NBA Stats](https://stats.nba.com/)。数据通过 `nba_api`
包进行下载，本项目设计了单独的类 [load_data.py](src/data/load_data.py) 进行原始数据下载与管理。

接口函数汇总参考 [API_doc](/doc/API.md)。