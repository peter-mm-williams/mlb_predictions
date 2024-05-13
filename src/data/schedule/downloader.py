from requests.exceptions import HTTPError
from typing import List, Dict, Any
import json
from logs import general_logger as logger
import time
import os
import statsapi
import pandas as pd


class SeasonDownloader:

    def __init__(self, season: int, save_dir: str):
        self.season = season
        self.save_dir = save_dir

    def get_games(self):
        games = statsapi.schedule(
            start_date=f'03/01/{self.season}', end_date=f'10/31/{self.season}')
        return games

    def download_and_save_games(self, max_tries: int = 5):
        cnt = 0
        while cnt < max_tries:
            try:
                self.save_games(self.get_games())
                logger.info(
                    f'Successfully downloaded schedule for {self.season}')
                break
            # 500 Server Error
            except HTTPError as e:
                cnt += 1
                logger.error(
                    f'HTTP Error while downloading schedule for {self.season};\n{e}\n sleeping 5 seconds and trying again')
                time.sleep(5)
            except Exception as e:
                logger.error(f'Exception: {e} while downloading {self.season}')
                break
        time.sleep(3)

    def save_games(self, games: List[Dict[str, Any]]):
        with open(f'{self.save_dir}games_{self.season}.json', 'w') as f:
            json.dump(games, f)


class Downloader:

    def __init__(self, start_season: int, save_dir: str):
        self.start_season = start_season
        self.save_dir = save_dir
        self.in_season = False
        self.seasons = []
        self._get_and_set_end_season_and_is_active()
        self._set_seasons_to_download()

    def get_downloaded_seasons(self):
        return [int(file.split('_')[1].split('.')[0]) for file in os.listdir(self.save_dir) if file.startswith('games')]

    def _set_seasons_to_download(self):
        downloaded_seasons = self.get_downloaded_seasons()
        self.seasons = list(set(self.seasons) - set(downloaded_seasons))
        if self.in_season:
            self.seasons = list(
                set(self.seasons).intersection({self.end_season}))
        self.seasons.sort()

    def download_games(self, max_tries: int = 5):
        for season in self.seasons:
            dl = SeasonDownloader(season, self.save_dir)
            dl.download_and_save_games(max_tries=max_tries)

    def _get_and_set_end_season_and_is_active(self):
        # Load in season start and end dates and toay's date
        cur_season: Dict[str, Any] = statsapi.latest_season()
        reg_start = pd.to_datetime(cur_season['regularSeasonStartDate'])
        season_end = pd.to_datetime(cur_season['postSeasonEndDate'])
        today = pd.to_datetime('today')

        # Set end season and in_season
        self.end_season = reg_start.year if today > reg_start else reg_start.year - 1
        self.in_season = (today > reg_start) and (today < season_end)
        self.seasons = list(range(self.start_season, self.end_season + 1))


if __name__ == '__main__':
    from pathlib import Path
    base_dir = str(Path(Path.cwd()).resolve().parent)
    Downloader(2010, f'{base_dir}/data/raw/schedule/').download_games()
