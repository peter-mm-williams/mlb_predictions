
from dataclasses import dataclass,  fields
from datetime import datetime as DateTimeType
from src.data.models.game import Game


@dataclass(init=True)
class GameLine:
    game: Game
    home_moneyline: float
    away_moneyline: float
    home_spread: float
    away_spread: float
    home_spread_odds: float
    away_spread_odds: float
    over_under: float
    over_odds: float
    under_odds: float

    def to_dict(self):
        return {
            'home_moneyline': self.home_moneyline,
            'away_moneyline': self.away_moneyline,
            'home_spread': self.home_spread,
            'away_spread': self.away_spread,
            'home_spread_odds': self.home_spread_odds,
            'away_spread_odds': self.away_spread_odds,
            'over_under': self.over_under,
            'over_odds': self.over_odds,
            'under_odds': self.under_odds
        } | self.game.to_dict()
