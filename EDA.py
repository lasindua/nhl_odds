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

game_log_pointer = db['team_game_logs'].find({}, {'_id':0})

game_log_data = list(game_log_pointer)

game_log_df = pd.DataFrame(game_log_data)

shot_pointer = db['shot_log'].find({}, {'_id':0})
shot_data = list(shot_pointer)

shot_df = pd.DataFrame(shot_data)

print('Game Log DataFrame')
print(game_log_df.head())

print('Shot Log DataFrame')
print(shot_df.head())

game_log_df = game_log_df['gameId'].apply(lambda x: f'{int(str(x)[:4])}0{int(str(x)[4:]):04d}')

merged_df = pd.merge(
    shot_df,
    game_log_df,
    left_on = 'shot_game_ID',
    right_on = 'gameId',
    how = 'left',
    suffixes=('_shot','_game')
)

ordered_columns = ['shooterPlayerId', 'shot_game_ID'] + \
                  [col for col in merged_df.columns if col not in ['shooterPlayerId', 'shot_game_ID']]
merged_df = merged_df[ordered_columns]

# Step 3: Group by shooterPlayerId and shot_game_ID (repeated measures structure)
repeated_measures_df = merged_df.groupby(['shooterPlayerId', 'shot_game_ID']).apply(
    lambda x: x.reset_index(drop=True)
).reset_index(drop=True)

print("Repeated Measures DataFrame")
print(repeated_measures_df.head())
