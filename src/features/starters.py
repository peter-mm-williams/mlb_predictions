import pandas as pd
import numpy as np
from pandas import DataFrame as DataFrameType
from typing import List
from datetime import timedelta
from logs.configurations.sports_logging import general_logger as logger


class StarterSeason:

    def __init__(self, starter_df: DataFrameType, season: int, pitcher_id: int, numbers_of_days: List[int]):
        '''
        Inputs:
        pitcher_df: DataFrame of all starter game logs for a season containing the following columns:
            ['game_id', 'game_datetime', 'season', 'personId', 'GS', 'ip', 'er', 'h', 'bb', 'k', 'hr', 'p', 's']
        season: int of the season to calculate the stats
        pitcher_id: int of the pitcher to calculate the stats (from statsapi)
        numbers_of_days: List of ints of the number of days to calculate the last n game stats

        Calculate the cumulutaive and last n game stats for a pitcher over a season for the following stats:
        1. IP/start (game)
        2. pitches/start
        3. strikes/start
        4. strike ratio (s/p)
        5. Strikeouts/9 IP (k)
        6. Walks/9 IP (bb)
        7. Homeruns/ 9 IP (r)
        8. WHIP
        9. ERA
        10. starts
        11. days rest
        '''
        self.season = season
        self.pitcher_id = pitcher_id
        self.game_stats = ['GS', 'ip', 'er', 'h', 'bb', 'k', 'hr', 'p', 's']
        self.starter_df = starter_df[(starter_df.personId == pitcher_id) & (starter_df.season == season)].reset_index(
            drop=True)
        self.numbers_of_days = numbers_of_days

    def _add_derived_columns(self, stat_df: DataFrameType, suffix: str):
        stat_df[f'ip_ave_{suffix}'] = stat_df[f'ip_{suffix}'] / \
            stat_df[f'GS_{suffix}']
        stat_df[f'p_start_ave_{suffix}'] = stat_df[f'p_{suffix}'] / \
            stat_df[f'GS_{suffix}']
        stat_df[f's_start_ave_{suffix}'] = stat_df[f's_{suffix}'] / \
            stat_df[f'GS_{suffix}']
        stat_df[f'strike_ratio_{suffix}'] = stat_df[f's_{suffix}'] / \
            stat_df[f'p_{suffix}']
        stat_df[f'k9_{suffix}'] = (
            stat_df[f'k_{suffix}'] / stat_df[f'ip_{suffix}']) * 9
        stat_df[f'bb9_{suffix}'] = (
            stat_df[f'bb_{suffix}'] / stat_df[f'ip_{suffix}']) * 9
        stat_df[f'hr9_{suffix}'] = (
            stat_df[f'hr_{suffix}'] / stat_df[f'ip_{suffix}']) * 9
        stat_df[f'whip_{suffix}'] = (
            stat_df[f'h_{suffix}'] + stat_df[f'bb_{suffix}']) / stat_df[f'ip_{suffix}']
        stat_df[f'era_{suffix}'] = (
            stat_df[f'er_{suffix}'] / stat_df[f'ip_{suffix}']) * 9
        return stat_df

    def get_cum_stats(self):
        stat_df = self.starter_df.copy()
        # Intermediate Columns: IP, ER, BBs, H, Ks, HRs,
        stat_df['days_rest'] = stat_df['game_datetime'].diff().dt.round(
            'D')  # .days
        for stat in self.game_stats:
            stat_df[f'{stat}_cum'] = stat_df[stat].astype(
                float).cumsum().shift(1)
        stat_df = self._add_derived_columns(stat_df, 'cum')
        return stat_df

    def _add_sum_cols(self, stat_df: DataFrameType, number_of_days: int, suffix: 'str'):
        sum_stat_cols = [f'{col}_{suffix}' for col in self.game_stats]
        stat_df[sum_stat_cols] = np.nan
        for i, (index, row) in enumerate(stat_df.iterrows()):
            # Define the date range
            start_date = row['game_datetime'] - timedelta(days=number_of_days)
            end_date = row['game_datetime']

            # Filter rows within the date range
            mask = (stat_df['game_datetime'] >= start_date) & (
                stat_df['game_datetime'] < end_date)
            filtered_df = stat_df.loc[mask][self.game_stats].copy().rename(
                columns=lambda x: x + f'_{suffix}')

            # Sum the specified columns
            sum_values_df = filtered_df[sum_stat_cols].astype(
                float).sum(axis=0)
            stat_df.iloc[i, -len(self.game_stats):] = sum_values_df.values
        return stat_df

    def get_last_n_stats(self, stat_df: DataFrameType, number_of_days: int):
        suffix = f'{number_of_days}_days'
        stat_df = self._add_sum_cols(stat_df, number_of_days, suffix)
        stat_df = self._add_derived_columns(stat_df, suffix)
        return stat_df

    def get_season_df(self):
        stat_df = self.get_cum_stats()
        for number_of_days in self.numbers_of_days:
            stat_df = self.get_last_n_stats(stat_df, number_of_days)
        return stat_df


class Starters:
    """
    Class to aggregate starting pitcher stats over a season to get the cumulative and last n game stats for the following stats:
    1. IP/start (game)
    2. pitches/start
    3. strikes/start
    4. strike ratio (s/p)
    5. Strikeouts/9 IP (k)
    6. Walks/9 IP (bb)
    7. Homeruns/ 9 IP (r)
    8. WHIP
    9. ERA
    10. starts
    11. days rest

    Inputs:
    pitcher_df: DataFrame of all starter game logs for a season containing the following columns:
        ['game_id', 'personId', 'GS', 'ip', 'er', 'h', 'bb', 'k', 'hr', 'p', 's']

    schedule_df: DataFrame of the schedule for a season containing at minimum the following columns:
        ['season', 'game_id', 'game_datetime']

    Outputs:
        DataFrame of the aggregated stats for each pitcher in the season
    """

    def __init__(self, pitcher_df: DataFrameType, schedule_df: DataFrameType):
        self.starter_df = pitcher_df[pitcher_df.GS].reset_index(drop=True)
        self.schedule_df = schedule_df
        self.starter_df = self.starter_df.merge(
            self.schedule_df[['season', 'game_id', 'game_datetime']], on='game_id', how='left'
        ).sort_values('game_datetime').reset_index(drop=True)
        self.seasons = schedule_df.season.unique()

    def get_pitcher_ids_in_season(self, season: int):
        return self.starter_df[self.starter_df.season == season].personId.unique()

    def aggregate_stats(self, numbers_of_days: List[int]):
        df = pd.DataFrame()
        for season in self.seasons:
            p_ids = self.get_pitcher_ids_in_season(season)
            for ind, p_id in enumerate(p_ids):
                logger.info(
                    f'Getting stats for pitcher {p_id} ({ind}/{len(p_ids)}) in season {season}')
                df = pd.concat((df, StarterSeason(
                    self.starter_df, season, p_id, numbers_of_days).get_season_df()))
        return df
