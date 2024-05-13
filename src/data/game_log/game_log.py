from typing import Dict, Literal, Any, List
import pandas as pd


class GameLog:

    def __init__(self, game_id: int, game_log_data: Dict[str, Any]):
        self.game_id = game_id
        self.game_log_data = game_log_data

    def get_game_info(self) -> Dict[str, Any]:
        return self.game_log_data['gameBoxInfo']

    def get_team_id(self, home_or_away: Literal['home', 'away']) -> int:
        return self.game_log_data['teamInfo'][home_or_away]['id']

    def get_team_abbrev(self, home_or_away: Literal['home', 'away']) -> str:
        return self.game_log_data['teamInfo'][home_or_away]['abbreviation']

    def get_team_name(self, home_or_away: Literal['home', 'away']) -> str:
        return self.game_log_data['teamInfo'][home_or_away]['teamName']

    def get_team_dict(self, home_or_away: Literal['home', 'away']) -> Dict[str, Any]:
        return {'team_id': self.get_team_id(home_or_away),
                'team_abbrev': self.get_team_abbrev(home_or_away),
                'team_name': self.get_team_name(home_or_away)
                }

    def get_batters(self, home_or_away: Literal['home', 'away']) -> List[Dict[str, Any]]:
        return [bat | self.get_team_dict(home_or_away)
                for bat in self.game_log_data[f'{home_or_away}Batters'][1:]]

    def get_pitchers(self, home_or_away: Literal['home', 'away']) -> List[Dict[str, Any]]:
        return [{'GS': ind == 0} | pitch | self.get_team_dict(home_or_away)
                for ind, pitch in enumerate(self.game_log_data[f'{home_or_away}Pitchers'][1:])]

    @property
    def start_time(self) -> str:
        return self.get_game_info()['startTime']

    @property
    def pitchers(self) -> List[Dict[str, Any]]:
        return self.get_pitchers('away') + self.get_pitchers('home')

    @property
    def batters(self) -> List[Dict[str, Any]]:
        return self.get_batters('away') + self.get_batters('home')

    @property
    def batters_df(self) -> pd.DataFrame:
        df = pd.DataFrame(self.batters)
        df['game_id'] = self.game_id
        return df

    @property
    def pitchers_df(self) -> pd.DataFrame:
        df = pd.DataFrame(self.pitchers)
        df['game_id'] = self.game_id
        return df
