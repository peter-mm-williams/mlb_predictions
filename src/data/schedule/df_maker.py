from typing import List, Optional, Dict, Any
import json
from datetime import datetime
import pandas as pd
import numpy as np


class DfMaker:

    def __init__(self, data_dir: str, seasons: List[int] = list(range(2010, 2024))):
        self.seasons = seasons
        self.data_dir = data_dir
        self.games = []
        self.int_cols = ['home_score', 'away_score']
        self.flt_cols = ['current_inning']
        self.date_cols = ['game_date', 'game_datetime']

    def load_games(self) -> List[Dict[str, Any]]:
        schedule = [json.load(open(
            f'{self.data_dir}/raw/schedule/games_{season}.json', 'r')) for season in self.seasons]
        return [game for season in schedule for game in season]

    def _set_columns_types(self, df: pd.DataFrame) -> pd.DataFrame:
        df[self.int_cols] = df[self.int_cols].astype(int)
        df[self.flt_cols] = df[self.flt_cols].replace('', np.nan).astype(float)
        df[self.date_cols] = df[self.date_cols].apply(pd.to_datetime)
        return df

    def _filter_out_games_after_today(self, df: pd.DataFrame) -> pd.DataFrame:
        today = datetime.now().strftime('%Y-%m-%d')
        return df[df['game_date'] <= today]

    def _save_df(self, df: pd.DataFrame):
        min_date = df['game_date'].min().strftime('%Y%m%d')
        max_date = df['game_date'].max().strftime('%Y%m%d')
        df.to_feather(
            f'{self.data_dir}/interim/schedule_{min_date}_to_{max_date}.feather')
        df.to_feather(f'{self.data_dir}/interim/schedule.feather')

    def make_df(self):
        df = pd.DataFrame(self.load_games())
        df = self._set_columns_types(df)
        df = self._filter_out_games_after_today(df)
        df['season'] = df['game_date'].dt.year  # add season column
        self._save_df(df)
        return df
