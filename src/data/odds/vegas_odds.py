import pandas as pd
import numpy as np
from pandas import DataFrame as DataFrameType

# Vegas odds downloaded from https://sports-statistics.com/sports-data/mlb-historical-odds-scores-datasets/


def parse_vegas_date(date):
    date = str(date)
    return f'{date[:-2]}-{date[-2:]}'


def edit_ou_col(column_name: str):
    if 'home_OU' in column_name:
        return column_name.replace('home_OU', 'over')
    elif 'away_OU' in column_name:
        return column_name.replace('away_OU', 'under')
    return column_name


class VegasOdds:
    """
    Class to load, format, and save Vegas odds data from https://sports-statistics.com/sports-data/mlb-historical-odds-scores-datasets/
    Original xlsx files have been manually downloaded and saved in data/raw/odds/vegas-odds/

    Inputs:
        base_dir: str, path to project directory
        year: int, year to load data

    Outputs:
        odds_df: DataFrame, formatted and saved odds data with the following columns:
            ['game', 'date', 'home_team', 'away_team', 'home_starter',
                'away_starter', 'home_moneyline_open', 'away_moneyline_open',
                'home_moneyline_close', 'away_moneyline_close', 'home_spread',
                'home_spread_odds', 'away_spread', 'away_spread_odds', 'over_open',
                'over_odds_open', 'under_open', 'under_odds_open', 'over_close',
                'over_odds_close', 'under_close', 'under_odds_close']
    """

    def __init__(self, base_dir: str, year: int):
        self.base_dir = base_dir
        self.year = year
        self.filename = f'{base_dir}/data/raw/odds/vegas-odds/mlb-odds-{year}.xlsx'

    def load_df(self):
        return pd.read_excel(self.filename)

    def _convert_halfs_to_numeric(self, df: DataFrameType) -> DataFrameType:
        return df.replace({f'{num}½': num+0.5 for num in range(0, 20)})

    def _fill_NL(self, df: DataFrameType) -> DataFrameType:
        return df.replace('NL', np.nan)

    def _parse_game_id(self, df: DataFrameType) -> DataFrameType:
        df['game'] = df.index // 2 + 1
        return df

    def _reformat_date(self, df: DataFrameType) -> DataFrameType:
        df['Date'] = pd.to_datetime(
            str(self.year) + '-' + df['Date'].apply(parse_vegas_date))
        return df

    def _rename_cols(self, df: DataFrameType) -> DataFrameType:
        return df.rename(columns={
            'Open': 'moneyline_open',
            'Close': 'moneyline_close',
            'Run Line': 'spread',
            'Run Line Odds': 'spread_odds',
            'Open OU Odds': 'OU_odds_open',
            'Close OU Odds': 'OU_odds_close',
            'Open OU': 'OU_open',
            'Close OU': 'OU_close',
            'Team': 'team',
            'Date': 'date',
            'Pitcher': 'starter'
        })

    def _split_home_away_and_rejoin(self, df: DataFrameType) -> DataFrameType:
        ha_cols = ['team', 'starter', 'moneyline_open',
                   'moneyline_close', 'spread', 'spread_odds', 'OU_open', 'OU_odds_open',
                   'OU_close', 'OU_odds_close']
        ha_cols = [col for col in ha_cols if col in df.columns]
        home_df = df[df['VH'] == 'H'][ha_cols + ['game', 'date']].rename(
            columns={col: f'home_{col}' for col in ha_cols}
        )
        away_df = df[df['VH'] == 'V'][ha_cols + ['game', 'date']].rename(
            columns={col: f'away_{col}' for col in ha_cols}
        )
        odds_df = home_df.merge(away_df, on=['game', 'date'])
        cols = [
            'game', 'date', 'home_team', 'away_team', 'home_starter',  'away_starter', 'home_moneyline_open', 'away_moneyline_open',
            'home_moneyline_close', 'away_moneyline_close',  'home_spread', 'home_spread_odds', 'away_spread', 'away_spread_odds',
            'home_OU_open', 'home_OU_odds_open', 'away_OU_open', 'away_OU_odds_open', 'home_OU_close', 'home_OU_odds_close',
            'away_OU_close', 'away_OU_odds_close'
        ]
        cols = [col for col in cols if col in odds_df.columns]
        ou_col_dict = {col: edit_ou_col(col)
                       for col in odds_df.columns if 'OU' in col}
        return odds_df[cols].rename(columns=ou_col_dict)

    def load_format_and_save(self) -> DataFrameType:
        df = self.load_df()
        df.columns = [col if 'Unnamed' not in col else df.columns[i-1] +
                      ' Odds' for i, col in enumerate(df.columns)]
        df = self._convert_halfs_to_numeric(df)
        df = self._parse_game_id(df)
        df = self._reformat_date(df)
        df = self._fill_NL(df)
        df = self._rename_cols(df)
        odds_df = self._split_home_away_and_rejoin(df)
        ml_cols = ['home_moneyline_open', 'away_moneyline_open',
                   'home_moneyline_close', 'away_moneyline_close']
        odds_df[ml_cols] = odds_df[ml_cols].astype(
            float)
        odds_df.reset_index(drop=True).to_feather(
            f'{self.base_dir}/data/interim/vegas-odds-{self.year}.feather')
        return odds_df


if __name__ == '__main__':
    from pathlib import Path
    base_dir = str(Path(Path.cwd()).resolve().parent)
    years = np.arange(2010, 2022)
    dfs = [VegasOdds(base_dir, year).load_format_and_save() for year in years]
    veg_df = pd.concat(dfs)
    veg_df.reset_index(drop=True).to_feather(
        f'{base_dir}/data/interim/vegas-odds.feather')
