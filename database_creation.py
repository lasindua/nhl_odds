import requests
import time
import pandas as pd
import zipfile
from io import BytesIO, StringIO
from pymongo import MongoClient
from pymongo import UpdateOne
from pymongo import InsertOne
from pymongo.server_api import ServerApi
from pymongo.errors import DuplicateKeyError
from concurrent.futures import ThreadPoolExecutor, as_completed

mongo_uri = 'mongodb+srv://lasindu:Hazelisdagoat1!@app-cluster.ckcue.mongodb.net/?retryWrites=true&w=majority&appName=app-cluster'
client = MongoClient(mongo_uri, server_api=ServerApi('1'))

# Testing connection with the database 
try:
    client.admin.command('ping')
    print('Pinged deployment. Successfully connected!')
except Exception as e:
    print(e)

# Creating a collection in the database for team info and grab player ID's
db = client['NHL']
info = db['roster_info']

team_list_codes = ["ANA", "BOS", "BUF", "CAR", "CBJ", "CGY", "CHI", "COL",
                   "DAL", "DET", "EDM", "FLA", "LAK", "MIN", "MTL", "NJD",
                   "NSH", "NYI", "NYR", "OTT", "PHI", "PIT", "SEA", "SJS",
                   "STL", "TBL", "TOR", "UTA", "VAN", "VGK", "WPG", "WSH"]

# Initialize list and formatted URL
team_url = 'https://api-web.nhle.com/v1/roster/{team}/20242025'

info.create_index('team_code', unique=True)

def fetch_team_info (team):
    url = team_url.format(team=team)
    try:
        response = requests.get(url)
        response.raise_for_status()
        team_rost = response.json()
        team_rost['team_code'] = team

        print(f'Fetched data for team {team}')
        return team_rost
    
    except requests.exceptions.RequestException as e:
        print(f'Failed to fetch data for team {team}: {e}')
        return None

def team_storage ():
    with ThreadPoolExecutor(max_workers=5) as executor:
        team_info = []

        futures = [executor.submit(fetch_team_info, team) for team in team_list_codes]

        for future in as_completed(futures):
            result = future.result()
            if result:
                team_info.append(result)

    if team_info:
        bulk_write = [
                UpdateOne(
                    {'team_code': team['team_code']},
                    {'$set': team},
                    upsert=True
                )
                for team in team_info
        ]
            
        if bulk_write:
            try:
                info.bulk_write(bulk_write)
                print(f'Bulk update complete: {result}')
            except Exception as e:
                print(f'Error handling database write: {e}')


stats = db['team_game_logs']
player_ids = []

for player in info.find():
    for position in ['forwards','defensemen','goalies']:
        if position in player:
            for id in player[position]:
                player_ids.append(id['id'])

print(f'Extracted {len(player_ids)} Player IDs')

money_puck = 'https://moneypuck.com/moneypuck/playerData/careers/gameByGame/all_teams.csv'
header = {'User-Agent':'Mozilla/5.0'}

def team_game_log(url, head):
    try:

        access = requests.get(url,headers=head)

        if access.status_code == 200:
            MP_file = pd.read_csv(StringIO(access.text))
            MP_file_filtered = MP_file[
                (MP_file['season'].isin([2022,2023,2024])) &
                 (MP_file['situation'] =='all')]
            MP_dict = MP_file_filtered.to_dict(orient='records')


            bulk_ops = [InsertOne(records) for records in MP_dict]
            stats.bulk_write(bulk_ops)
            print(f'Completed team bulk write')

            stats.create_index('gameId')
        else:
            print(f'Unable to fetch data. Status code: {access.status_code}')
    except Exception as e:
        print(f'An error occured: {e}')
    



'''
current_season = 20242025
fetch_season = [current_season, 20232024, 20222023]
game_log_url = 'https://api-web.nhle.com/v1/player/{player_id}/game-log/{season}/2'

def game_log_fetch (player_id):
    player_doc = {'player_id': player_id, 'game_logs':{}}
    
    for season in fetch_season:    
        game_url = game_log_url.format(player_id=player_id, season=season)
        try:
            response = requests.get(game_url)
            response.raise_for_status()
            game_log = response.json()
            
            if 'gameLog' in game_log:
                player_doc['game_logs'][str(season)] = game_log['gameLog']
                print(f'Fetched game logs for player {player_id}, from the {season} season.')
            else:
                print(f'"game_log" not found for player {player_id}, season {season}. Response: {game_log}')
                player_doc['game_logs'][str(season)] = []
        except requests.exceptions.RequestException as e:
            print(f'Failed to fetch game logs for player {player_id}, from the {season} season: {e}')
    return player_doc

def game_log_storage ():
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = [executor.submit(game_log_fetch, player_id) for player_id in player_ids]

        season_game_logs = []
        for future in as_completed(futures):
            result = future.result()
            if result:
                season_game_logs.append(result)

    if season_game_logs:
        bulk_operations = []
        for player_games in season_game_logs:
            bulk_operations.append(
                UpdateOne(
                    {'player_id':player_games['player_id']},
                    {'$set': player_games},
                    upsert=True
                )
            )
        start_time = time.time()
        if bulk_operations:
            try:
                result = stats.bulk_write(bulk_operations)
                print(f'Bulk update complete: {result}')
            except Exception as e:
                print(f'Error handling database write: {e}')
        end_time = time.time()
        elapsed_time = end_time - start_time
        print(f'Bulk write took {elapsed_time:.3f} seconds to execute')
'''


shot_zip_files = ['https://peter-tanner.com/moneypuck/downloads/shots_2022.zip',
                  'https://peter-tanner.com/moneypuck/downloads/shots_2023.zip',
                  'https://peter-tanner.com/moneypuck/downloads/shots_2024.zip']


shot_log = db['shot_log']

columns_to_extract = ['shooterPlayerId', 'xGoal', 'shotWasOnGoal','goal', 'homeTeamCode', 'awayTeamCode', 'shotID', 'season', 'game_id']

def zip_process (zip_url):
    try:
        print(f'Processing zip file from: {zip_url}')
        response = requests.get(zip_url)
        response.raise_for_status()
    
        with zipfile.ZipFile(BytesIO(response.content)) as z:
            file_names = z.namelist()
            print(f'Files in the zip file: {file_names}')


            for file in file_names:
                if file.endswith('.csv'):
                    print(f'Processing {file}...')
                    with z.open(file) as f:
                        df = pd.read_csv(f, usecols = columns_to_extract)

                        df['shot_game_ID'] = df.apply(
                            lambda row: f"{int(row['season'])}0{int(row['game_id']):04d}", axis=1
                        )

                        converted_df = df.to_dict(orient='records')

                        season = converted_df[0]['season']

                        start_time = time.time()

                        bulk_ops = [InsertOne(record) for record in converted_df]
                        shot_log.bulk_write(bulk_ops)

                        end_time = time.time()
                        elapsed_time = end_time - start_time
                        print(f'Bulk write took: {elapsed_time:.3f} to complete')
            
            shot_log.create_index('shooterPlayerID')
            shot_log.create_index('shot_game_ID')
        print(f'Completed processing zip file: {zip_url}')

    except requests.exceptions.RequestException as e:
        print(f'Failed to download zip file from {zip_url}: {e}')

    except zipfile.BadZipFile as e:
        print(f'Invalid zip file from {zip_url}: {e}')
    
    except ValueError as e:
        print(f'Error while processing CSV: {e}')
        


stats.drop()
print(f'Deleted all records from collection')

shot_log.drop()
print(f'Dropped all files from collection')
if __name__ == '__main__':
    print('Starting the script to fetch and store team data...')

    team_storage()

    team_game_log(money_puck, header)

    for zip_url in shot_zip_files:
        zip_process(zip_url)
    print('Script execution complete')


