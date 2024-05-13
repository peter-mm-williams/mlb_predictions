from src.data.models import Player, Team, Weather, Stadium, Game
import pandas as pd
from typing import Dict, Any, List
from logs import logger_bp as logger


class BettingProsGameParser:
    def __init__(self, json_data: Dict[str, Any]):
        self.data = json_data

    def parse_team(self, team_data):
        team_id = team_data['id']
        team_name = team_data['name']
        team_record = team_data['team']['record'] if 'record' in list(
            team_data['team'].keys()) else {'W': 0, 'L': 0}
        city = team_data['team']['city'] if 'city' in list(
            team_data['team'].keys()) else ''
        abbreviation = team_data['team']['abbreviation']
        return Team(team_id, city, abbreviation, team_name, team_record)

    def parse_player(self, player_data):
        if player_data is None:
            return Player(-1, '', '', '', '', '', '')
        bp_id = player_data['id'] if 'id' in list(player_data.keys()) else -1
        first_name = player_data['player']['first_name'] if 'first_name' in list(
            player_data['player'].keys()) else ''
        last_name = player_data['player']['last_name'] if 'last_name' in list(
            player_data['player'].keys()) else ''
        position = player_data['player']['position'] if 'position' in list(
            player_data['player'].keys()) else ''
        team = player_data['player']['team'] if 'team' in list(
            player_data['player'].keys()) else ''
        conference = player_data['player']['conference'] if 'conference' in list(
            player_data['player'].keys()) else ''
        division = player_data['player']['division'] if 'division' in list(
            player_data['player'].keys()) else ''
        return Player(bp_id, first_name, last_name, position, team, conference, division)

    def parse_weather(self, weather_data):
        temperature = weather_data.get('forecast_temp')
        wind_speed = weather_data.get('forecast_wind_speed')
        wind_direction = weather_data.get('forecast_wind_direction')
        wind_degree = weather_data.get('forecast_wind_degree')
        pressure = weather_data.get('forecast_pressure')
        precipitation_chance = weather_data.get('forecast_rain_chance')
        humidity = weather_data.get('forecast_humidity')
        return Weather(temperature, wind_speed, wind_direction, wind_degree, pressure, precipitation_chance, humidity)

    def parse_stadium(self, venue_data):
        name = venue_data.get('name')
        id = venue_data.get('id')
        city = venue_data.get('city')
        state = venue_data.get('state')
        surface = venue_data.get('surface')
        capacity = venue_data.get('capacity')
        stadium_type = venue_data.get('stadium_type')

        return Stadium(id, name, city, state, stadium_type, capacity, surface)

    def parse_lineup(self, lineup_data):
        return [self.parse_player(player_data['participant']) for player_data in lineup_data] if lineup_data else []

    def get_game(self):
        try:
            bp_id = self.data['id']
            home_team_data = next(
                participant for participant in self.data['participants'] if participant['id'] == self.data['home'])
            home_team = self.parse_team(home_team_data)
            away_team_data = next(
                participant for participant in self.data['participants'] if participant['id'] == self.data['visitor'])
            away_team = self.parse_team(away_team_data)
            weather = self.parse_weather(self.data['weather'])
            stadium = self.parse_stadium(self.data['venue'])
            date = pd.to_datetime(self.data['scheduled'])
            if 'home_starter' in list(self.data['pitchers'].keys()):
                home_starting_pitcher = self.parse_player(
                    self.data['pitchers']['home_starter'])
            elif 'home_probable' in list(self.data['pitchers'].keys()):
                home_starting_pitcher = self.parse_player(
                    self.data['pitchers']['home_probable'])
            else:
                logger.info(
                    f'No starting pitcher found for home team in game {bp_id}; {self.data["pitchers"]}')
                home_starting_pitcher = None
            if 'visitor_starter' in list(self.data['pitchers'].keys()):
                away_starting_pitcher = self.parse_player(
                    self.data['pitchers']['visitor_starter'])
            elif 'visitor_probable' in list(self.data['pitchers'].keys()):
                away_starting_pitcher = self.parse_player(
                    self.data['pitchers']['visitor_probable'])
            else:
                logger.info(
                    f'No starting pitcher found for away team in game {bp_id}; {self.data["pitchers"]}')
                away_starting_pitcher = None
            home_lineup = self.parse_lineup(
                self.data['lineups']['home_lineup'])
            away_lineup = self.parse_lineup(
                self.data['lineups']['visitor_lineup'])
            return Game(home_team, away_team, date, weather, stadium, bp_id, home_starting_pitcher, away_starting_pitcher, home_lineup, away_lineup)
        except Exception as e:
            logger.error(f'Error parsing game data: {e}')
            raise e
