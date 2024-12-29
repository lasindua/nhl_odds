import requests
from pymongo import MongoClient
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
team_info = []
team_url = 'https://api-web.nhle.com/v1/roster/{team}/20232024'

info.create_index('team_code', unique=True)
for team in team_list_codes:
    url = team_url.format(team=team)
    try:
        response = requests.get(url)
        response.raise_for_status()
        team_rost = response.json()
        team_rost['team_code'] = team
        info.insert_one(team_rost)
        print(f'Fetched data for team {team}')
    except DuplicateKeyError:
        print(f'Data for team {team} already exists. Skipping')
    except requests.exceptions.RequestException as e:
        print(f'Failed to fetch data for team {team}: {e}')


delete_result = info.delete_many({"team_code": {"$exists": False}})
print(f'Deleted {delete_result.deleted_count} documents without "team_code"')

pipeline = [
    {'$group':
     {'_id': '$team_code',
      'count':{'$sum':1},
      'docs': {'$push': '$_id'}
      }},
      {'$match': {'count':{'$gt': 1}}}
]
duplicates = list(info.aggregate(pipeline))
for dup in duplicates:
    remove_id = dup['docs'][1:]
    info.delete_many({'_id': {'$in': remove_id}})

print(f'Removed duplicate documents')

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

            player_doc['game_logs'][str(season)] = game_log['gameLog']
            print(f'Fetched game logs for player {player_id}, from the {season} season.')
        except requests.exceptions.RequestException as e:
            print(f'Failed to fetch game logs for player {player_id}, from the {season} season: {e}')
    return player_doc

with ThreadPoolExecutor(max_workers=10) as executor:
    futures = [executor.submit(game_log_fetch, player_id) for player_id in player_ids]

    season_game_logs = []
    for future in as_completed(futures):
        result = future.result()
        if result:
            season_game_logs.append(result)

if season_game_logs:
    stats.insert_many(season_game_logs)
    print(f'Inserted {len(season_game_logs)} player game log documents.')
