# NBA 数据分析

## 数据来源

数据原本应该通过调用
`nba_api` 这个接口来获取，但是由于接口的问题，中国区调取数据比较麻烦，用梯子下载也容易被拉黑。所以这里退而求其次，在 Kaggle 上下载了[数据集](https://www.kaggle.com/datasets/wyattowalsh/basketball)。这个数据集截止到2023年，不过并不妨碍我们对数据进行分析。

值得注意的是，这里的数据提供了两种格式，一种是 `.csv` 存储的各个表格，一种是
`.sqlite` 存储的数据库文件。在这个项目中，为了达到锻炼人的目的，我选择手搓 sql 代码。通过调用 python 和 sql 的接口来获取数据。

### 表格含义

这个表格整理了各个表格的名称及其数据含义。当然这也只是一部分，如果需要更多的数据，还是要调用
`nba_api` 来获取。不过在我们的研究范围内，这些数据也是足够用了。

|        表格名称         | 数据内容                 |
|:-------------------:|:---------------------|
| common_player_info  | 球员基本信息，包括球员身高、体重、id等 |
| draft_combine_stats | 球员选秀时的各项数据信息         |
|    draft_history    | 历届选秀数据               |
|        game         | 历史比赛数据               |
|      game_info      | 每场比赛的观众数与时长          |
|    game_summary     | 每场比赛转播情况             |
|  inactive_players   | 每场比赛登场球员信息           |
|     line_score      | 每场比赛每节的数据信息          |
|      officials      | ~~应该是~~工作人员的信息       |
|     other_stats     | 通常包含二阶数据             |
|    play_by_play     | 比赛过程的回合详细信息          |
|       player        | 球员名称及在役情况            |
|        team         | 联盟三十支球队的信息           |
|    team_detailis    | 联盟三十支球队的详细信息         |
|    team_history     | 球队历史数据               |
|  team_info_common   | ~~球队信息，但是空表~~        |