from typing import Dict, Any, List, Tuple
from src.data.models import GameLine, Game
from logs import logger_bp as logger
import numpy as np


class GameLinesParser:

    def __init__(self, line_data: Dict[str, Any], game: Game):
        self.line_data = line_data
        self.game = game
        self.home_team = game.home_team.abbreviation
        self.away_team = game.away_team.abbreviation
        self.market_dict = {
            122: 'moneyline',
            175: 'total runs',
            176: 'spread'
        }
        self.market_ids = list(self.market_dict.keys())

    def _parse_bet_from_selection(self, selection: Dict[str, Any], market_id: int):
        bet = selection['selection'] if market_id == 175 else selection['participant']
        bet = 'home' if bet == self.home_team else 'away' if bet == self.away_team else bet
        return bet

    def parse_selection(self, selection: Dict[str, Any], market_id: int) -> Tuple[str, Dict[str, Any]]:
        try:
            dk_selection = next(
                line for line in selection['books'] if line['id'] == 12)  # DraftKings
            for line in dk_selection['lines']:
                if "replaced_by" in line.keys():
                    dk_selection['lines'].remove(line)
            odds = dk_selection['lines'][0]['cost']
            line = dk_selection['lines'][0]['line']
        except:
            logger.error(
                f'Error parsing selection {selection} for market {market_id}\n could not find book id of 12', exc_info=True)
            odds, line = np.nan, np.nan
        return (self._parse_bet_from_selection(selection, market_id), {
            'market_id': market_id,
            'odds': odds,
            'line': line
        })

    def parse_market(self, market_id: int):
        selections = next(
            offer for offer in self.line_data['offers'] if offer['market_id'] == market_id)['selections']
        return dict(self.parse_selection(selection, market_id) for selection in selections)

    def parse_markets(self):
        try:
            betting_line_dict = {self.market_dict[market_id]: self.parse_market(
                market_id) for market_id in self.market_ids}
            return GameLine(
                self.game,
                betting_line_dict['moneyline']['home']['odds'],
                betting_line_dict['moneyline']['away']['odds'],
                betting_line_dict['spread']['home']['line'],
                betting_line_dict['spread']['away']['line'],
                betting_line_dict['spread']['home']['odds'],
                betting_line_dict['spread']['away']['odds'],
                betting_line_dict['total runs']['over']['line'],
                betting_line_dict['total runs']['over']['odds'],
                betting_line_dict['total runs']['under']['odds']
            )
        except Exception as ex:
            logger.error(
                f'Error parsing game lines for game {self.game.bp_id}\n\t{self.line_data}', exc_info=True)
            raise ex
