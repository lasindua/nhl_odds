import time
import pandas as pd
from pymongo import MongoClient
from pymongo.server_api import ServerApi
import statsmodels.api as sm
import statsmodels.formula.api as smf
from statsmodels.tools.sm_exceptions import ConvergenceWarning



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

shot_df = shot_df[(shot_df['shooterPlayerId'] != 0) & (shot_df['shooterPlayerId'].notna())]
shot_df['shooterPlayerId'] = shot_df['shooterPlayerId'].astype(int)

game_log_df = game_log_df['gameId'].apply(lambda x: f'{int(str(x)[:4])}0{int(str(x)[4:]):04d}')

merged_df = pd.merge(
    game_log_df,
    shot_df,
    left_on = 'gameId',
    right_on = 'shot_game_ID',
    how = 'left',
    suffixes=('_shot','_game')
)
print('Merged Dataframe')
print(merged_df.head())

duplicates = merged_df.duplicated(subset=['shooterPlayerId', 'shot_game_ID', 'shotID'])
print(f"Number of exact duplicates: {duplicates.sum()}")
merged_df = merged_df.drop_duplicates(subset=['shooterPlayerId', 'shot_game_ID', 'shotID'])

duplicate_check = merged_df.groupby(['shooterPlayerId', 'shot_game_ID', 'shotID']).size()



ordered_columns = ['shooterPlayerId', 'shot_game_ID', 'shotID'] + \
                  [col for col in merged_df.columns if col not in ['shooterPlayerId', 'shot_game_ID', 'shotID']]
merged_df = merged_df[ordered_columns]


# Step 3: Group by shooterPlayerId and shot_game_ID (repeated measures structure)
repeated_measures_df = merged_df.groupby(['shooterPlayerId', 'shot_game_ID', 'shotID']).apply(
    lambda x: x.reset_index(drop=True)
).reset_index(drop=True)

print("Repeated Measures DataFrame")
print(repeated_measures_df.head())

repeated_measures_df['season'] = repeated_measures_df['season'].astype('category')

model_1 = smf.mixedlm("goal ~ xGoal + shotWasOnGoal", repeated_measures_df, groups=repeated_measures_df['season'])
result = model_1.fit()
print(result.summary())
