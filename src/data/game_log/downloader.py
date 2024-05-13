from typing import List, Dict, Any
from logs.configurations.sports_logging import general_logger as logger
from src.data.game_log import GameLog
from requests.exceptions import HTTPError
from typing import List, Dict, Any
import json
from logs import general_logger as logger
import time
import os
import statsapi


class GameDownloader:
    """
    Class to download a single game log from statsapi.mlb.com

    Inputs:
        game_id is the game_id for the game log to download
        save_dir is the directory to save the game log
        season is the season for the game log to download
    """

    def __init__(self, game_id: int, save_dir: str, season: int):
        self.game_id = game_id
        self.save_dir = save_dir
        self.season = season
        self.save_path = f'{self.save_dir}/{self.season}'
        self.pitch_file = f'{self.save_path}/pitchers_{self.game_id}.feather'
        self.bat_file = f'{self.save_path}/batters_{self.game_id}.feather'

    def download_game(self, game_id: int):
        return statsapi.boxscore_data(game_id)

    def download_game_with_retries(self, max_tries: int = 3):
        cnt = 0
        while cnt < max_tries:
            try:
                return self.download_game(self.game_id)
            except HTTPError as e:  # 500 Server Error
                cnt += 1
                logger.error(
                    f'HTTP Error while downloading box score for game_id: {self.game_id};\n{e}\n sleeping 5 seconds and trying again')
                time.sleep(5)
            except Exception as e:
                logger.error(
                    f'Exception: {e} while downloading box score for game: {self.game_id}')
                break

    def parse_game(self, game_log_data: Dict[str, Any]) -> GameLog:
        return GameLog(self.game_id, game_log_data)

    def save_pitchers_and_batters(self, game_log: GameLog, overwrite: bool = False):
        if not os.path.exists(self.save_path):
            os.makedirs(self.save_path)
        game_log.pitchers_df.reset_index(
            drop=True).to_feather(self.pitch_file)
        game_log.batters_df.reset_index(
            drop=True).to_feather(self.bat_file)

    @property
    def pitch_file_exists(self) -> bool:
        return os.path.exists(self.pitch_file)

    @property
    def bat_file_exists(self) -> bool:
        return os.path.exists(self.bat_file)

    def download_and_save_game(self, max_tries: int = 3, overwrite: bool = False):
        if not overwrite and self.bat_file_exists and self.pitch_file_exists:
            # if both files exist and the overwrite flag is set to false,
            #     then we do not re-download the game
            logger.info(f'Already downloaded game_id: {self.game_id}')
            return
        game = self.download_game_with_retries(max_tries=max_tries)
        if game is not None:
            gl = self.parse_game(game)
            self.save_pitchers_and_batters(gl, overwrite=overwrite)


class Downloader:
    '''
    Class to download game logs from statsapi.mlb.com

    Inputs:
        game_ids is a dictionary with years as keys and a list of game_ids for each season as values
        save_dir is the directory to save the game logs
    '''

    def __init__(self, game_ids: Dict[int, List[int]], save_dir: str):
        self.game_ids = game_ids
        self.save_dir = save_dir

    def download_season(self, year: int):
        n_games = len(self.game_ids[year])
        for i, game_id in enumerate(self.game_ids[year]):
            gd = GameDownloader(game_id, self.save_dir, year)
            gd.download_and_save_game()
            if i % 25 == 0:
                logger.info(
                    f'{100.*i/n_games} % complete - Finished downloading {i} of {n_games} games for {year}')
        logger.info(f'Finished downloading season: {year}')

    def download_seasons(self):
        for year in self.game_ids.keys():
            self.download_season(year)
