from logs import logger_bp as logger
from src.data.models import Game, GameLine
from src.data.odds import GameLinesParser, BettingProsGameParser
import requests
from typing import List, Dict, Any, Optional
import pandas as pd
from pandas import DataFrame as DataFrameType


class BettingPros:

    def __init__(self, date: Optional[str] = None):
        self.date = date
        self.base_url = 'https://api.bettingpros.com/'

    def construct_url(self, purpose, event_ids: Optional[List[int]] = None, localize: bool = False):
        endpoint_dict = {
            'event': 'v3/events?sport=MLB',
            'market': 'v3/offers?sport=MLB&market_id=287',
            'gameline': 'v3/offers?market_id=122:175:176&sport=MLB&live=true'
        }
        url = self.base_url + endpoint_dict[purpose]
        url += f'&date={self.date}' if self.date else ''
        if event_ids:
            url += f'&event_id={":".join([str(event_id) for event_id in event_ids])}'
        if localize:
            url += '&location=CT'
        return url

    def fetch_games_data(self) -> Dict[str, Any]:
        return self.make_request(self.construct_url('event'))

    def fetch_hits_over_under_data(self, event_ids: Optional[List[int]]) -> Dict[str, Any]:
        return self.make_request(self.construct_url('market', event_ids=event_ids))

    def fetch_game_lines_data(self, event_ids: Optional[List[int]]) -> Dict[str, Any]:
        return self.make_request(self.construct_url('gameline', event_ids=event_ids))

    def make_request(self, url: str) -> Dict[str, Any]:
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:89.0) Gecko/20100101 Firefox/89.0",
            "x-api-key": 'CHi8Hy5CEE4khd46XNYL23dCFX96oUdw6qOt1Dnh'
        }
        r = requests.get(url, headers=headers)
        try:
            return r.json()
        except Exception as e:
            logger.error(f'Error making request to {url}', exc_info=True)
            return {'failure_message': str(e)}

    def get_games(self) -> List[Game]:
        games_data: List[Dict[str, Any]] = self.fetch_games_data()['events']
        return [BettingProsGameParser(game_data).get_game() for game_data in games_data]

    def _fetch_and_get_gameline(self, game: Game) -> GameLine:
        game_data: Dict[str, Any] = self.fetch_game_lines_data(event_ids=[
                                                               game.bp_id])
        return GameLinesParser(game_data, game).parse_markets()

    def get_gamelines(self) -> List[GameLine]:
        return [self._fetch_and_get_gameline(game) for game in self.get_games()]

    def get_and_save_gameline_df(self, data_dir: str) -> DataFrameType:
        df = pd.DataFrame([game_line.to_dict()
                          for game_line in self.get_gamelines()])
        df.to_feather(
            f'{data_dir}/raw/odds/bettingpros/{self.date}_gamelines.feather')
        return df
