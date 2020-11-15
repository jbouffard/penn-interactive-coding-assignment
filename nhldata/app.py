'''
	This is the NHL crawler.  

Scattered throughout are TODO tips on what to look for.

Assume this job isn't expanding in scope, but pretend it will be pushed into production to run 
automomously.  So feel free to add anywhere (not hinted, this is where we see your though process..)
    * error handling where you see things going wrong.  
    * messaging for monitoring or troubleshooting
    * anything else you think is necessary to have for restful nights
'''
import logging
from io import StringIO
from pathlib import Path
from datetime import datetime
from dataclasses import dataclass
import boto3
import requests
import pandas as pd
from botocore.config import Config
from dateutil.parser import parse as dateparse

from nhldata.parser import NhlParser
from nhldata.schema import NHL_SCHEMA
from nhldata.strategy import NHL_PARSING_STRATEGY


logging.basicConfig(level=logging.INFO)
LOG = logging.getLogger(__name__)


class NHLApi:
    SCHEMA_HOST = "https://statsapi.web.nhl.com/"
    VERSION_PREFIX = "api/v1"

    def __init__(self, base=None):
        self.base = base if base else f'{self.SCHEMA_HOST}/{self.VERSION_PREFIX}'


    def schedule(self, start_date: datetime, end_date: datetime) -> dict:
        '''
        returns a dict tree structure that is like
            "dates": [
                {
                    " #.. meta info, one for each requested date ",
                    "games": [
                        { #.. game info },
                        ...
                    ]
                },
                ...
            ]
        '''
        LOG.info("Attempting to fetch NHL game schedules from {} to {}".format(start_date, end_date))

        try:
            result = self._get(
                self._url('schedule'),
                {'startDate': start_date.strftime('%Y-%m-%d'),
                 'endDate': end_date.strftime('%Y-%m-%d')}
            )

        except Exception:
            LOG.error("Failed to fetch NHL game schedules from {} to {}".format(start_date, end_date), exc_info=True)

            result = {}

        return result

    def boxscore(self, game_id):
        '''
        returns a dict tree structure that is like
           "teams": {
                "home": {
                    " #.. other meta ",
                    "players": {
                        $player_id: {
                            #... player info
                        },
                        #...
                    }
                },
                "away": {
                    #... same as "home"
                }
            }
        '''
        LOG.info("Attempting to fetch boxscore for game {}".format(game_id))

        url = self._url(f'game/{game_id}/boxscore')

        try:
            result = self._get(url)
        except Exception:
            LOG.error("Unable to fetch boxscore for game {}".format(game_id))

            result = {}

        return result

    def _get(self, url, params=None):
        response = requests.get(url, params=params, timeout=10.0)
        response.raise_for_status()
        return response.json()

    def _url(self, path):
        return f'{self.base}/{path}'

@dataclass
class StorageKey:
    gameid: str
    playerid: str

    def key(self):
        ''' renders the s3 key for the given set of properties '''
        return f'{int(self.playerid)}/{self.gameid}.csv'

class Storage():
    def __init__(self, dest_bucket, s3_client):
        self._s3_client = s3_client
        self.bucket = dest_bucket

    def store_game(self, key: StorageKey, game_data) -> bool:
        csv_buffer = StringIO()
        game_data.to_csv(csv_buffer)
        self._s3_client.put_object(Bucket=self.bucket, Key=key.key(), Body=csv_buffer.getvalue())
        return True

class Crawler():
    def __init__(self, api: NHLApi, storage: Storage, version: str):
        self.api = api
        self.storage = storage
        self.parser = NhlParser(NHL_PARSING_STRATEGY[version], NHL_SCHEMA[version])
        self.output_cols = self.parser.output_cols

    def crawl(self, startDate: datetime, endDate: datetime) -> None:
        LOG.info("Crawling for NHL data from {} to {}".format(startDate, endDate))

        schedule = self.api.schedule(startDate, endDate)
        dates = schedule.get('dates')

        if not schedule or not dates:
            LOG.warn("Unable able to find any NHL game data between {} and {}".format(startDate, endDate))

        else:
            game_ids = self.parser.parse_game_id(dates)

            for game_id in game_ids:
                boxscore = self.api.boxscore(game_id)
                player_dfs = self.parser.parse_player_info(boxscore, self.output_cols)

                for player_df in player_dfs:
                    key = StorageKey(game_id, player_df.iloc[0]['player_person_id'])

                    self.storage.store_game(key, player_df)

	# NOTE the data direct from the API is not quite what we want. Its nested in a way we don't want
	#      so here we are looking for your ability to gently massage a data set. 
        #TODO error handling
        #TODO get games for dates
        #TODO for each game get all player stats: schedule -> date -> teams.[home|away] -> $playerId: wanted_data
        #TODO output to S3 should be a csv that matches the schema of utils/create_games_stats 

def main():
    import os
    import argparse

    parser = argparse.ArgumentParser(description='NHL Stats crawler')
    parser.add_argument("--start_date", default="2020-08-04", type=str)
    parser.add_argument("--end_date", default="2020-08-05", type=str)
    parser.add_argument("--version", default="v1", type=str)
    args = parser.parse_args()

    version = args.version

    raw_start_date = args.start_date
    #raw_start_date = "2020-08-04"
    raw_end_date = args.end_date
    #raw_end_date = "2020-08-05"

    startDate = datetime.strptime(raw_start_date, "%Y-%m-%d")
    endDate = datetime.strptime(raw_end_date, "%Y-%m-%d")

    dest_bucket = os.environ.get('DEST_BUCKET', 'output')

    api = NHLApi()

    s3client = boto3.client('s3', config=Config(signature_version='s3v4'), endpoint_url=os.environ.get('S3_ENDPOINT_URL'))

    storage = Storage(dest_bucket, s3client)
    crawler = Crawler(api, storage, version)
    crawler.crawl(startDate, endDate)

if __name__ == '__main__':
    main()
