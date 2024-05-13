from src.data.models.player import Player
from src.data.models.team import Team
from src.data.models.weather import Weather
from src.data.models.stadium import Stadium
from typing import List, Optional
from dataclasses import dataclass,  fields
from datetime import datetime as DateTimeType


@dataclass(init=True)
class Game:
    home_team: Team
    away_team: Team
    date: DateTimeType
    weather: Weather
    stadium: Stadium
    bp_id: int
    home_starting_pitcher: Optional[Player]
    away_starting_pitcher: Optional[Player]
    home_lineup: List[Player]
    away_lineup: List[Player]
    home_score: int = 0
    away_score: int = 0

    def to_dict(self):
        return {
            'bp_id': self.bp_id,
            'home_team': self.home_team.to_dict(),
            'away_team': self.away_team.to_dict(),
            'date': self.date,
            'home_starting_pitcher': self.home_starting_pitcher.to_dict() if self.home_starting_pitcher else None,
            'away_starting_pitcher': self.away_starting_pitcher.to_dict() if self.away_starting_pitcher else None,
            'home_lineup': [player.to_dict() for player in self.home_lineup],
            'away_lineup': [player.to_dict() for player in self.away_lineup],
        } | self.weather.to_dict() | self.stadium.to_dict()
