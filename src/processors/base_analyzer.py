import os

import pandas as pd


class BaseAnalyzer:
    """所有分析器的基类，共享数据库对象和通用过滤条件"""

    def __init__(self, db_manager):
        self.db = db_manager

    def _load_data(self, query, params=None):
        """通用的数据加载方法"""
        return self.db.query(query, params)

    @staticmethod
    def _save_data(df: pd.DataFrame, class_name, task_name):
        """
        通用的数据保存方式
        :param df: 需要保存的数据
        :param class_name: 任务类型的名称
        :param task_name: 任务名称
        """
        save_path = os.path.join("data/processed", class_name)
        os.makedirs(save_path, exist_ok=True)

        # 路径拼接和添加文件扩展名
        file_path = os.path.join(save_path, f"{task_name}.csv")
        df.to_csv(file_path, index=False)
        print(f"✅ 数据已保存至: {file_path}")
