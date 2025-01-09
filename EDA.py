import time
import pandas as pd
from pymongo import MongoClient
from pymongo.server_api import ServerApi



mongo_uri = 'mongodb+srv://lasindu:Hazelisdagoat1!@app-cluster.ckcue.mongodb.net/?retryWrites=true&w=majority&appName=app-cluster'
client = MongoClient(mongo_uri, server_api=ServerApi('1'))

try:
    client.admin.command('ping')
    print('Pinged deployment. Successfully connected!')
except Exception as e:
    print(e)

db = client['NHL']

game_log_pointer = db['player_game_logs'].find({}, {'_id':0})

game_log_data = list(game_log_pointer)

game_log_df = pd.DataFrame(game_log_data)

shot_pointer = db['shot_log'].find({}, {'_id':0})
shot_data = list(shot_pointer)

shot_df = pd.DataFrame(shot_data)

print('Game Log DataFrame')
print(game_log_df.head())

print('Shot Log DataFrame')
print(shot_df.head())

group_shot_log = shot_df.groupby(['shooterPlayerId', 'season']).apply(lambda x: x.to_dict(orient='records')).reset_index(name = 'shot_logs')

group_shot_log = group_shot_log.groupby('shooterPlayerId'
                                        ).apply(lambda x: {season: logs for season, logs in zip(x['season'], x['shot_logs'])}
                                        ).reset_index(name='game_logs')
merged_df = pd.merge(
    game_log_df,
    group_shot_log,
    left_on = 'player_id',
    right_on = 'shooterPlayerId',
    how = 'left'
)

print(merged_df.head())