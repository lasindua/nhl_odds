import requests
import time
import pandas as pd
from pymongo import MongoClient
from pymongo import UpdateOne
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


stats = db['player_game_logs']
player_ids = []

for player in info.find():
    for position in ['forwards','defensemen','goalies']:
        if position in player:
            for id in player[position]:
                player_ids.append(id['id'])

print(f'Extracted {len(player_ids)} Player IDs')

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

if __name__ == '__main__':
    print('Starting the script to fetch and store team data...')
    team_storage()
    game_log_storage()
    print('Script execution complete')


money_puck = pd.read_csv('https://moneypuck.com/moneypuck/playerData/careers/gameByGame/all_teams.csv')