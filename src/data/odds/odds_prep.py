from pandas import DataFrame as DataFrameType
import pandas as pd
from sklearn import base


class OddsPrep:

    def __init__(self, base_dir: str):
        self.base_dir = base_dir
        self.vegas_odds_df = pd.DataFrame()
        self.bp_df = pd.DataFrame()
        self.all_odds_df = pd.DataFrame()

    def load_data(self):
        self.vegas_odds_df = pd.read_feather(
            f'{self.base_dir}/data/interim/vegas-odds.feather')
        self.bp_df = pd.read_feather(
            f'{self.base_dir}/data/interim/bp_gamelines.feather')
        self.schedule_df = pd.read_feather(
            f'{self.base_dir}/data/interim/schedule.feather')

    def _prep_bp_for_concat(self) -> DataFrameType:
        temp_df = self.bp_df[[
            'date', 'home_team', 'away_team', 'home_moneyline', 'away_moneyline',
            'home_spread', 'away_spread', 'home_spread_odds', 'away_spread_odds',
            'over_under', 'over_odds', 'under_odds'
        ]].copy()
        temp_df['home_name'] = temp_df['home_team'].apply(
            lambda x: x['abbreviation'])
        temp_df['away_name'] = temp_df['away_team'].apply(
            lambda x: x['abbreviation'])
        return temp_df

    def _prep_vegas_for_concat(self) -> DataFrameType:
        cols = ['date', 'home_name', 'away_name', 'home_moneyline', 'away_moneyline', 'home_spread', 'away_spread',
                'home_spread_odds', 'away_spread_odds', 'over_under', 'over_odds',
                'under_odds']
        return self.vegas_odds_df.rename(columns={
            'home_moneyline_close': 'home_moneyline',
            'away_moneyline_close': 'away_moneyline',
            'over_close': 'over_under',
            'over_odds_close': 'over_odds',
            'under_odds_close': 'under_odds',
            'home_team': 'home_name',
            'away_team': 'away_name'
        })[cols]

    def prep_and_concat_odds(self):
        v_odds_df = self._prep_vegas_for_concat()
        bp_odds_df = self._prep_bp_for_concat()
        # remove overlapping dates; defer to betting pros
        v_odds_df = v_odds_df[v_odds_df['date'] <= bp_odds_df['date'].min()]
        self.all_odds_df = pd.concat(
            (v_odds_df, bp_odds_df)).reset_index(drop=True)
        self.all_odds_df['date'] = pd.to_datetime(self.all_odds_df['date'])
        self.all_odds_df

    def clean_odds_df(self, max_moneyline: float = 250, min_moneyline: float = -250, max_abs_spread: float = 3) -> DataFrameType:
        self.all_odds_df = self.all_odds_df[
            (self.all_odds_df['home_moneyline'].between(min_moneyline, max_moneyline)) &
            (self.all_odds_df['away_moneyline'].between(min_moneyline, max_moneyline)) &
            (self.all_odds_df['home_spread'].between(-max_abs_spread, max_abs_spread)) &
            (self.all_odds_df['away_spread'].between(-max_abs_spread, max_abs_spread))
        ]
        return self.all_odds_df

    def save_cleaned_odds(self):
        self.all_odds_df.reset_index(drop=True).to_feather(
            f'{self.base_dir}/data/interim/all-odds.feather')

    def run(self):
        self.load_data()
        self.prep_and_concat_odds()
        self.clean_odds_df()
        self.save_cleaned_odds()


if __name__ == '__main__':
    base_dir = '../../'
    all_odds_df = pd.read_feather(f'{base_dir}/data/interim/all-odds.feather')
    pm_df = pd.read_feather(
        f'{base_dir}/data/interim/pitching-matchups.feather')
    schedule_df = pd.read_feather(f'{base_dir}/data/interim/schedule.feather')
    matchup_abbrev_dict = {
        'FLA': 'MIA'
    }

    pm_df = pm_df.replace(
        {'home_abbrev': matchup_abbrev_dict, 'away_abbrev': matchup_abbrev_dict})
    pm_df['game_datetime'] = pd.to_datetime(
        pm_df['game_datetime'], utc=True).dt.tz_convert('US/Eastern')
    pm_df['game_date'] = pd.to_datetime(pm_df['game_datetime'].copy()).dt.date

    odds_to_matchup_abbr_dict = {
        'ARI': 'AZ',
        'CUB': 'CHC',
        'KAN': 'KC',
        'LOS': 'LAD',
        'SDG': 'SD',
        'SFO': 'SF',
        'TAM': 'TB',
        'WAS': 'WSH'
    }

    all_odds_df['game_date'] = pd.to_datetime(all_odds_df['date']).dt.date
    all_odds_df = all_odds_df.rename(
        columns={'home_name': 'home_abbrev', 'away_name': 'away_abbrev'})
    all_odds_df = all_odds_df.replace(
        {'home_abbrev': odds_to_matchup_abbr_dict, 'away_abbrev': odds_to_matchup_abbr_dict})

    all_odds_df['time'] = pd.to_datetime(all_odds_df['date']).dt.time
    all_odds_df['matchup'] = all_odds_df['game_date'].astype(
        str) + '-' + all_odds_df['away_abbrev'] + '@' + all_odds_df['home_abbrev']

    # Games with Times
    no_time_games_df = all_odds_df[all_odds_df.time.astype(
        str) == '00:00:00'].copy()
    time_games_df = all_odds_df[all_odds_df.time.astype(
        str) != '00:00:00'].copy()

    # Merge Timed Games with Pitching Matchups
    time_games_df['game_datetime'] = pd.to_datetime(
        time_games_df['date'], utc=True).dt.tz_convert('US/Eastern')
    time_games_df = time_games_df.drop(
        columns=['game_date', 'date', 'home_team',
                 'away_team', 'time', 'matchup']
    ).merge(pm_df, on=['game_datetime', 'home_abbrev', 'away_abbrev'])

    # Identify double headers in games with no time (i.e. matchup occurs twice in a day)
    matchup_counts = no_time_games_df.matchup.value_counts()
    double_header_matchups = matchup_counts[matchup_counts > 1]
    single_game_matchups = matchup_counts[matchup_counts == 1]

    # Merge single games with pm_df
    single_game_df = no_time_games_df[no_time_games_df.matchup.isin(
        single_game_matchups.index)]
    single_game_df = single_game_df.drop(columns=['date', 'home_team', 'away_team', 'time', 'matchup']).merge(
        pm_df, on=['game_date', 'home_abbrev', 'away_abbrev'])

    # Merge double header games with pm_df
    double_header_df = no_time_games_df[no_time_games_df.matchup.isin(
        double_header_matchups.index)].copy()
    double_header_df['game_num'] = double_header_df.groupby(
        'matchup').cumcount()

    # Add index to double header games in pm_df
    pm_df = pm_df.merge(schedule_df[['game_id', 'game_num']], on='game_id')
    double_header_df = double_header_df.merge(
        pm_df, on=['game_date', 'home_abbrev', 'away_abbrev', 'game_num']
    ).drop(columns=['game_num', 'date', 'home_team', 'away_team', 'time', 'matchup'])

    df = pd.concat((time_games_df, single_game_df, double_header_df)).reset_index(
        drop=True).sort_values('game_datetime')
