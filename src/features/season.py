from typing import List, Dict, Any
import pandas as pd
import numpy as np


class TeamSeason:
    def __init__(self, season_df: pd.DataFrame, team_id: int):
        self.team_id = team_id
        self.season_df = season_df.copy()
        self.cols = [
            'season',
            'game_id',
            'game_date',
            'game_datetime',
            'opponent_id',
            'runs_for',
            'runs_against',
            'if_home'
        ]
        self.cum_stats = ['runs_for', 'runs_against', 'run_differential']

    def _get_home_games(self):
        home_df = self.season_df[self.season_df['home_id']
                                 == self.team_id].copy()
        home_df['if_home'] = True
        rename_dict = {
            'away_id': 'opponent_id',
            'home_score': 'runs_for',
            'away_score': 'runs_against'
        }
        return home_df.rename(columns=rename_dict)[self.cols]

    def _get_away_games(self):
        away_df = self.season_df[self.season_df['away_id']
                                 == self.team_id].copy()
        away_df['if_home'] = False
        rename_dict = {
            'home_id': 'opponent_id',
            'away_score': 'runs_for',
            'home_score': 'runs_against'
        }
        return away_df.rename(columns=rename_dict)[self.cols]

    def get_team_schedule_df(self):
        team_df = pd.concat([self._get_home_games(), self._get_away_games()],
                            axis=0).sort_values('game_datetime').reset_index(drop=True)
        team_df['team_id'] = self.team_id
        team_df['run_differential'] = team_df['runs_for'] - \
            team_df['runs_against']
        team_df['win'] = team_df['run_differential'] > 0
        team_df['loss'] = team_df['run_differential'] < 0
        team_df['wins'] = team_df['win'].cumsum()
        team_df['losses'] = team_df['loss'].cumsum()
        team_df['games_played'] = team_df[['wins', 'losses']].sum(axis=1)
        team_df['win_pct'] = team_df['wins'] / team_df['games_played']
        mean_stats = [stat+'_mean' for stat in self.cum_stats]
        std_stats = [stat+'_std' for stat in self.cum_stats]
        team_df[mean_stats] = team_df[self.cum_stats].expanding().mean().shift(1)
        team_df[std_stats] = team_df[self.cum_stats].expanding().std().shift(1)
        team_df['win_streak'] = (team_df['win'].cumsum() -
                                 team_df['win'].cumsum().where(~team_df['win']).ffill().fillna(0)).shift(1)
        team_df['loss_streak'] = (team_df['loss'].cumsum() -
                                  team_df['loss'].cumsum().where(~team_df['loss']).ffill().fillna(0)).shift(1)
        return team_df

    def make_last_n_stats(self, team_df: pd.DataFrame, n: int):
        last_n_df = team_df.copy()
        mean_stats = [stat+'_mean_last_'+str(n) for stat in self.cum_stats]
        std_stats = [stat+'_std_last_'+str(n) for stat in self.cum_stats]
        last_n_df[mean_stats] = last_n_df[self.cum_stats].rolling(
            n).mean().shift(1)
        last_n_df[std_stats] = last_n_df[self.cum_stats].rolling(
            n).std().shift(1)
        last_n_df['wins_last_' +
                  str(n)] = last_n_df['win'].rolling(n).sum().shift(1)
        last_n_df['losses_last_' +
                  str(n)] = last_n_df['loss'].rolling(n).sum().shift(1)
        last_n_df['win_pct_last_'+str(n)] = last_n_df['wins_last_'+str(n)] / n
        return last_n_df

    def get_team_season_df(self, last_ns: List[int]):
        team_df = self.get_team_schedule_df()
        for n in last_ns:
            team_df = self.make_last_n_stats(team_df, n)
        return team_df


class Season:
    def __init__(self, schedule_df: pd.DataFrame, season: int, include_today: bool = False):
        self.season = season
        # Take regular season games of the season of interest that have be played to completion
        status_options = [
            'Final',
            'Completed Early: Rain',
            'Completed Early',
            'Completed Early: Wet Grounds'
        ]
        if include_today:
            status_options += ['Pre-Game', 'Scheduled']
        self.season_df = schedule_df.loc[(
            (schedule_df['game_type'] == 'R') &
            (schedule_df['season'] == season) &
            (schedule_df['status'].isin(status_options))
        ), :].copy()
        self.team_ids = self.season_df['home_id'].unique()

    def get_teams_season_df(self):
        return pd.concat(tuple(
            [TeamSeason(self.season_df, team_id).get_team_season_df([5, 10])
             for team_id in self.team_ids]
        ))

    def get_matchups_df(self, last_ns: List[int]):
        basic_cols = ['season', 'game_id', 'team_id']
        team_stat_cols = [
            'games_played',
            'win_pct',
            'runs_for_mean',
            'runs_against_mean',
            'run_differential_mean',
            'runs_for_std',
            'runs_against_std',
            'run_differential_std',
            'win_streak',
            'loss_streak'
        ]
        last_n_stat_cols = [
            'runs_for_mean',
            'runs_for_std',
            'runs_against_mean',
            'runs_against_std',
            'run_differential_mean',
            'run_differential_std', 'win_pct'
        ]
        recent_stat_cols = [stat+'_last_' +
                            str(n) for stat in last_n_stat_cols for n in last_ns]
        team_cols = basic_cols + team_stat_cols + recent_stat_cols
        sched_cols = [
            'game_id',
            'game_num',
            'game_date',
            'game_datetime',
            'home_id',
            'away_id',
            'home_name',
            'away_name',
            'home_score',
            'away_score'
        ]

        team_season_df = self.get_teams_season_df()

        home_team_df = team_season_df[team_season_df['if_home']][team_cols].rename(columns={'team_id': 'home_id'} | {
            col: 'home_' + col for col in team_stat_cols + recent_stat_cols
        })
        away_team_df = team_season_df[~team_season_df['if_home']][team_cols].rename(columns={'team_id': 'away_id'} | {
            col: 'away_' + col for col in team_stat_cols + recent_stat_cols
        })
        season_df = self.season_df[sched_cols].merge(
            home_team_df, on=['game_id',  'home_id'], how='right').merge(
            away_team_df, on=['season', 'game_id', 'away_id'], how='right'
        ).sort_values('game_datetime')

        game_time = season_df.game_datetime.dt.tz_convert('US/Pacific')
        season_df['minutes_pacific'] = (
            game_time).dt.hour*60 + game_time.dt.minute
        return season_df.dropna().reset_index(drop=True)
