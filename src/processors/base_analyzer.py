class BaseAnalyzer:
    """所有分析器的基类，共享数据库对象和通用过滤条件"""

    def __init__(self, db_manager):
        self.db = db_manager

    def _load_data(self, query, params=None):
        """通用的数据加载方法"""
        return self.db.query(query, params)
