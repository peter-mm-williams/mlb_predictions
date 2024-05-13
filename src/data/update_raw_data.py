import os
from turtle import up
from src.data.game_log import Downloader as GlDownloader
from src.data.schedule import DfMaker
from src.data.schedule import Downloader as SchDownloader
from src.data.odds.betting_pros import BettingPros
import pandas as pd
import numpy as np
import time
from datetime import timedelta
from pathlib import Path
from typing import Dict, List
from logs.configurations.sports_logging import general_logger as logger


class DataDownloader:

    def __init__(self):
        self.base_dir = str(Path(os.path.dirname(
            os.path.realpath(__file__))).resolve().parent.parent)
        self.schedule_df = pd.DataFrame()

    def update_schedule(self):
        SchDownloader(
            2010, f'{self.base_dir}/data/raw/schedule/').download_games()
        self.schedule_df = DfMaker(f'{self.base_dir}/data/').make_df()

    def _make_game_ids(self) -> Dict[int, List[int]]:
        return {
            year: list(self.schedule_df[(self.schedule_df.game_type == 'R') & (
                self.schedule_df.season == year)].game_id.unique())
            for year in self.schedule_df.season.unique()
        }

    def update_game_logs(self):
        dl = GlDownloader(self._make_game_ids(),
                          f'{self.base_dir}/data/raw/game_logs/')
        dl.download_seasons()

    def get_downloaded_odd_dates(self) -> List[str]:
        return [f.split('_')[0] for f in os.listdir(f'{self.base_dir}/data/raw/odds/bettingpros/')]

    def update_odds(self):
        dates = pd.to_datetime(self.schedule_df[self.schedule_df['game_date']
                               > '2019-01-01']['game_date']).dt.strftime('%Y-%m-%d')
        downloaded_dates = self.get_downloaded_odd_dates()
        dates = np.sort(list(set(dates) - set(downloaded_dates)))
        game_lines_dfs = []
        failed_dates = []
        t0 = time.time()
        for idx, date in enumerate(dates):
            try:
                game_lines_dfs += [BettingPros(
                    date).get_and_save_gameline_df(self.base_dir+'/data/')]
            except:
                failed_dates.append(date)
                logger.info(f'Failed to extract gamelines on {date}')
            if idx % 5 == 0:
                ti = time.time()
                logger.info(
                    f'Collected {len(game_lines_dfs)} games through {date}')
                logger.info(
                    f'\tTotal elapsed time: {timedelta(seconds=(ti-t0))}; Average time per date: {timedelta(seconds=(ti-t0)/(idx+1))}')
                time.sleep(3+np.random.randn())

    def update_elo(self):
        elo_df = pd.read_csv(
            'https://projects.fivethirtyeight.com/mlb-api/mlb_elo.csv')
        elo_df.to_feather(f'{self.base_dir}/data/raw/elo.feather')

    def update_all(self):
        spacer = '='*50
        t0 = time.time()

        logger.info(f'\n{spacer}Updating schedule{spacer}')
        self.update_schedule()
        t1 = time.time()
        logger.info(f'... done; took {timedelta(seconds=(t1-t0))}')

        logger.info(f'\n{spacer}Updating game logs{spacer}')
        self.update_game_logs()
        t2 = time.time()
        logger.info(f'... done; took {timedelta(seconds=(t2-t1))}')

        logger.info(f'\n{spacer}Updating odds{spacer}')
        self.update_odds()
        t3 = time.time()
        logger.info(f'... done; took {timedelta(seconds=(t3-t2))}')

        logger.info(f'\n{spacer}Updating elo{spacer}')
        self.update_elo()
        t4 = time.time()
        logger.info(f'... done; took {timedelta(seconds=(t4-t3))}')
        logger.info(
            f'\n{spacer}All updates complete; Total time: {timedelta(seconds=(t4-t0))}{spacer}')
        logger.info(spacer*2+'\n')


def main():
    dd = DataDownloader()
    dd.update_all()


if __name__ == '__main__':
    main()

"""
elo_df = pd.read_csv('https://projects.fivethirtyeight.com/mlb-api/mlb_elo.csv')
elo_to_samples = {
    'ANA': 'LAA',
    'ARI': 'AZ',
    'CHW': 'CWS',
    'FLA': 'MIA',
    'KCR': 'KC',
    'SDP': 'SD',
    'SFG': 'SF',
    'TBR': 'TB',
    'WSN': 'WSH'
}
elo_df = elo_df.replace(elo_to_samples).rename(columns=
    {
        'date':'game_date', 
        'team1':'home_abbrev', 
        'team2':'away_abbrev',
        'elo1_pre':'home_elo_pre',
        'elo2_pre':'away_elo_pre',
        'elo_prob1':'home_elo_prob',
        'elo_prob2':'away_elo_prob',
        'rating1_pre':'home_rating_pre',
        'rating2_pre':'away_rating_pre',
        'rating_prob1':'home_rating_prob',
        'rating_prob2':'away_rating_prob',
        'pitcher1_rgs':'home_pitcher_rgs',
        'pitcher2_rgs':'away_pitcher_rgs',
        'pitcher1_adj':'home_pitcher_adj',
        'pitcher2_adj':'away_pitcher_adj',
        'score1':'home_score',
        'score2':'away_score',
     }
).drop(columns = ['rating1_post','rating2_post', 'neutral', 'playoff'])
elo_df[elo_df.game_date>'2010']
"""
