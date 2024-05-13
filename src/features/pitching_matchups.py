from typing import List, Tuple, Literal
from pandas import DataFrame as DataFrameType
import pandas as pd
import numpy as np


class PitchingMatchups:
    '''
    Class which combines starter stats and matchups to create a dataframe containing the following features:
        'game_id',
        'game_date',
        'game_datetime',
        'home_id',
        'away_id',
        'home_name',
        'away_name',
        'home_score',
        'away_score',
        'season',
        'home_games_played',
        'home_win_pct',
        'home_runs_for_mean',
        'home_runs_against_mean',
        'home_run_differential_mean',
        'home_runs_for_std',
        'home_runs_against_std',
        'home_run_differential_std',
        'home_win_streak',
        'home_loss_streak',
        'home_runs_for_mean_last_5',
        'home_runs_for_mean_last_10',
        'home_runs_for_std_last_5',
        'home_runs_for_std_last_10',
        'home_runs_against_mean_last_5',
        'home_runs_against_mean_last_10',
        'home_runs_against_std_last_5',
        'home_runs_against_std_last_10',
        'home_run_differential_mean_last_5',
        'home_run_differential_mean_last_10',
        'home_run_differential_std_last_5',
        'home_run_differential_std_last_10',
        'home_win_pct_last_5',
        'home_win_pct_last_10',
        'away_games_played',
        'away_win_pct',
        'away_runs_for_mean',
        'away_runs_against_mean',
        'away_run_differential_mean',
        'away_runs_for_std',
        'away_runs_against_std',
        'away_run_differential_std',
        'away_win_streak',
        'away_loss_streak',
        'away_runs_for_mean_last_5',
        'away_runs_for_mean_last_10',
        'away_runs_for_std_last_5',
        'away_runs_for_std_last_10',
        'away_runs_against_mean_last_5',
        'away_runs_against_mean_last_10',
        'away_runs_against_std_last_5',
        'away_runs_against_std_last_10',
        'away_run_differential_mean_last_5',
        'away_run_differential_mean_last_10',
        'away_run_differential_std_last_5',
        'away_run_differential_std_last_10',
        'away_win_pct_last_5',
        'away_win_pct_last_10',

    The following stats for home and away starters:
        'GS_cum', 'ip_cum', 'er_cum', 'h_cum', 'bb_cum', 'k_cum', 'hr_cum',
            'p_cum', 's_cum', 'ip_ave_cum', 'p_start_ave_cum', 's_start_ave_cum',
            'strike_ratio_cum', 'k9_cum', 'bb9_cum', 'hr9_cum', 'whip_cum',
            'era_cum', 'GS_16_days', 'ip_16_days', 'er_16_days', 'h_16_days',
            'bb_16_days', 'k_16_days', 'hr_16_days', 'p_16_days', 's_16_days',
            'ip_ave_16_days', 'p_start_ave_16_days', 's_start_ave_16_days',
            'strike_ratio_16_days', 'k9_16_days', 'bb9_16_days', 'hr9_16_days',
            'whip_16_days', 'era_16_days', 'GS_31_days', 'ip_31_days', 'er_31_days',
            'h_31_days', 'bb_31_days', 'k_31_days', 'hr_31_days', 'p_31_days',
            's_31_days', 'ip_ave_31_days', 'p_start_ave_31_days',
            's_start_ave_31_days', 'strike_ratio_31_days', 'k9_31_days',
            'bb9_31_days', 'hr9_31_days', 'whip_31_days', 'era_31_days'
    '''

    def __init__(self, base_dir: str):
        self.base_dir = base_dir
        self.matchups_df = pd.DataFrame()
        self.starter_df = pd.DataFrame()
        self.info_cols = ['game_id', 'season', 'game_datetime']
        self.ha_cols = [
            'name', 'personId', 'team_id', 'team_abbrev', 'team_name',
            'days_rest', 'GS_cum', 'ip_cum', 'er_cum', 'h_cum', 'bb_cum', 'k_cum', 'hr_cum',
            'p_cum', 's_cum', 'ip_ave_cum', 'p_start_ave_cum', 's_start_ave_cum',
            'strike_ratio_cum', 'k9_cum', 'bb9_cum', 'hr9_cum', 'whip_cum',
            'era_cum', 'GS_16_days', 'ip_16_days', 'er_16_days', 'h_16_days',
            'bb_16_days', 'k_16_days', 'hr_16_days', 'p_16_days', 's_16_days',
            'ip_ave_16_days', 'p_start_ave_16_days', 's_start_ave_16_days',
            'strike_ratio_16_days', 'k9_16_days', 'bb9_16_days', 'hr9_16_days',
            'whip_16_days', 'era_16_days', 'GS_31_days', 'ip_31_days', 'er_31_days',
            'h_31_days', 'bb_31_days', 'k_31_days', 'hr_31_days', 'p_31_days',
            's_31_days', 'ip_ave_31_days', 'p_start_ave_31_days',
            's_start_ave_31_days', 'strike_ratio_31_days', 'k9_31_days',
            'bb9_31_days', 'hr9_31_days', 'whip_31_days', 'era_31_days'
        ]

    def load_data(self):
        self.matchups_df = pd.read_feather(
            f'{self.base_dir}/data/interim/matchups.feather')
        self.starter_df = pd.read_feather(
            f'{self.base_dir}/data/interim/starter_stats.feather')

    def _make_home_or_away_start_df(self, home_or_away: Literal['home', 'away']) -> DataFrameType:
        ha_cols_dict = {col: f'{home_or_away}_{col}' for col in self.ha_cols}
        ha_cols_dict.update({
            f'name': f'{home_or_away}_starter',
            f'team_id': f'{home_or_away}_id',
            f'team_name': f'{home_or_away}_name',
            f'team_abbrev': f'{home_or_away}_abbrev',
        })
        return self.starter_df.copy().rename(columns=ha_cols_dict)[self.info_cols + list(ha_cols_dict.values())]

    def _make_starter_matchup_df(self) -> DataFrameType:
        home_starter_df = self._make_home_or_away_start_df(
            'home').drop_duplicates()
        away_starter_df = self._make_home_or_away_start_df(
            'away').drop_duplicates()
        df = self.matchups_df.merge(home_starter_df.drop(columns=['home_name']), on=[
                                    'season', 'game_id', 'game_datetime', 'home_id'])
        return df.merge(away_starter_df.drop(columns=['away_name']), on=['season', 'game_id', 'game_datetime', 'away_id'])

    def save_pitching_matchups(self, df: DataFrameType):
        df.to_feather(
            f'{self.base_dir}/data/interim/pitching-matchups.feather')

    def get_pitching_matchups(self) -> DataFrameType:
        self.load_data()
        df = self._make_starter_matchup_df()
        self.save_pitching_matchups(df)
        return df
