from src.utils.db_utils import DatabaseManager
from .clutch_analyzer import ClutchAnalyzer
from .game_analyzer import GameAnalyzer
from .garbage_time_analyzer import GarbageTimeAnalyzer
from .player_analyzer import PlayerAnalyzer
from .statistics_analyzer import StatisticsAnalyzer
from .team_analyzer import TeamAnalyzer


class NBAAnalyzer:
    """统一的NBA分析器（兼容旧接口）"""

    def __init__(self, db_path):
        # 初始化数据管理模块
        self.db_manager = DatabaseManager(db_path)

        # 初始化分析模块
        self.player_analyzer = PlayerAnalyzer(self.db_manager)
        self.team_analyzer = TeamAnalyzer(self.db_manager)
        self.game_analyzer = GameAnalyzer(self.db_manager)
        self.stats_analyzer = StatisticsAnalyzer(self.db_manager)
        self.clutch_analyzer = ClutchAnalyzer(self.db_manager)
        self.garbage_time_analyzer = GarbageTimeAnalyzer(self.db_manager)

    def connect(self):
        """开启数据库链接"""
        self.db_manager.connect()

    def disconnect(self):
        """关闭数据库链接"""
        self.db_manager.close()

    # --- 接口转发区：可以在定义暴露给 main 里面的接口

    def analyze_duration(self):
        return self.game_analyzer.analyze_game_duration_trend()

    def analyze_home_advantage(self):
        return self.game_analyzer.home_advantage()

    def analyze_three_attempt_trend(self):
        return self.game_analyzer.three_attempt_trend()

    def analyze_player_clutch(self, player_id: int = None, player_name: str = None):
        return self.clutch_analyzer.analyze_player(
            player_id=player_id, player_name=player_name
        )

    def analyze_player_garbage_time(
        self, player_id: int = None, player_name: str = None
    ):
        return self.garbage_time_analyzer.analyze_player(
            player_id=player_id, player_name=player_name
        )
