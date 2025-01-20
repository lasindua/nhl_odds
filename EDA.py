import time
import pandas as pd
from pymongo import MongoClient
from pymongo.server_api import ServerApi
import statsmodels.api as sm
import statsmodels.formula.api as smf
from statsmodels.tools.sm_exceptions import ConvergenceWarning
import seaborn as sns
import matplotlib.pyplot as plt



mongo_uri = 'mongodb+srv://lasindu:Hazelisdagoat1!@app-cluster.ckcue.mongodb.net/?retryWrites=true&w=majority&appName=app-cluster'
client = MongoClient(mongo_uri, server_api=ServerApi('1'))

try:
    client.admin.command('ping')
    print('Pinged deployment. Successfully connected!')
except Exception as e:
    print(e)

db = client['NHL']

game_log_pointer = db['team_game_logs'].find({},{'_id':0})
game_log_data = list(game_log_pointer)
game_log_df = pd.DataFrame(game_log_data)



shot_pointer = db['shot_log'].find({}, {'_id':0})

shot_data = list(shot_pointer)

shot_df = pd.DataFrame(shot_data)

print('Game Log DataFrame')
print(type(game_log_df))
print(game_log_df.head())

print('Shot Log DataFrame')
print(type(shot_df))
print(shot_df.head())

# There were around 8 observations where the playerID was 0 or NA, so they are dropped
shot_df = shot_df[(shot_df['shooterPlayerId'] != 0) & (shot_df['shooterPlayerId'].notna())]

# Converting the string of playerID's to integers to merge
shot_df['shooterPlayerId'] = shot_df['shooterPlayerId'].astype(int)
print(shot_df.dtypes)

# Converted `gameId` to an object so that datatype matches with other df
game_log_df['gameId'] = game_log_df['gameId'].apply(lambda x: f'{int(str(x)[:4])}0{int(str(x)[4:]):04d}')
print(game_log_df.dtypes)

merged_df = pd.merge(
    game_log_df,
    shot_df,
    left_on = 'gameId',
    right_on = 'shot_game_ID',
    how = 'left',
    suffixes=('_player','_team')
)
print('Merged Dataframe')
print(merged_df.head())
merged_df['team_code'] = merged_df.apply(lambda x: x['homeTeamCode'] if x['isHomeTeam'] == 1 else x['awayTeamCode'], axis=1)
duplicates = merged_df.duplicated(subset=['shooterPlayerId', 'shot_game_ID', 'shotID'])
print(f"Number of exact duplicates: {duplicates.sum()}")
merged_df = merged_df.drop_duplicates(subset=['shooterPlayerId', 'shot_game_ID', 'shotID'])

duplicate_check = merged_df.groupby(['shooterPlayerId', 'shot_game_ID', 'shotID']).size()



ordered_columns = ['season_player','team_code', 'shooterPlayerId', 'shot_game_ID', 'shotID'] + \
                  [col for col in merged_df.columns if col not in ['season_player','team_code', 'shooterPlayerId', 'shot_game_ID', 'shotID']]
merged_df = merged_df[ordered_columns]

print(merged_df.head(10))
print(merged_df.columns)

# Group by shooterPlayerId and shot_game_ID (repeated measures structure)
repeated_measures_df = merged_df.copy()

print("Repeated Measures DataFrame")
print(repeated_measures_df.head(10))

# Creating composite grouping variables
repeated_measures_df['season_team'] = repeated_measures_df['season_player'].astype(str) + '_' + repeated_measures_df['team_code'].astype(str)
repeated_measures_df['season_team_player'] = repeated_measures_df['season_team'].astype(str) + '_' + repeated_measures_df['shooterPlayerId'].astype(str)
repeated_measures_df['season_team_game'] = repeated_measures_df['season_team'].astype(str) + '_' + repeated_measures_df['shot_game_ID'].astype(str)

# Converting categorical variables into the appropriate datatype
repeated_measures_df['season_player'] = repeated_measures_df['season_player'].astype('category')
repeated_measures_df['homeTeamCode'] = repeated_measures_df['homeTeamCode'].astype('category')
repeated_measures_df['awayTeamCode'] = repeated_measures_df['awayTeamCode'].astype('category')
repeated_measures_df['shooterPlayerId'] = repeated_measures_df['shooterPlayerId'].astype('category')
repeated_measures_df['shot_game_ID'] = repeated_measures_df['shot_game_ID'].astype('category')
repeated_measures_df['season_team'] = repeated_measures_df['season_team'].astype('category')
repeated_measures_df['season_team_player'] = repeated_measures_df['season_team_player'].astype('category')
repeated_measures_df['season_team_game'] = repeated_measures_df['season_team_game'].astype('category')
repeated_measures_df['shotWasOnGoal'] = repeated_measures_df['shotWasOnGoal'].astype('category')

print(repeated_measures_df[['season_team', 'team_code']].head(15))

repeated_measures_df = repeated_measures_df.drop(columns=['homeTeamCode', 'awayTeamCode', 'gameId'])

deduplicated_data = repeated_measures_df[['season_team', 'shot_game_ID', 'team_code', 'goal', 'xGoal']].drop_duplicates()

# Attemping to create a new dataframe with summed values for goal and xGoal
'''
game_level_data = (deduplicated_data.groupby(
    ['season_team', 'shot_game_ID','team_code'], as_index=False)
    .agg({
        'goal':'sum',
        'xGoal':'sum'
    })
)
''' 

game_level_data = repeated_measures_df.sort_values(['season_team','shot_game_ID'])

# 7-game rolling average for expected goals
game_level_data['xGoal_rolling_avg'] = (
    game_level_data.groupby('season_team')['xGoal']
    .rolling(window=7, min_periods=1)
    .mean()
    .reset_index(level=0, drop=True)
)

# 7-game rolling average for goals
game_level_data['goal_rolling_avg'] = (
    game_level_data.groupby('season_team')['goal']
    .rolling(window=7, min_periods=1)
    .mean()
    .reset_index(level=0, drop=True)
)

# Subsetting dataframe to only select teams from 2022 season
test_filter = game_level_data['season_team'].where(game_level_data['season_team'].str.startswith('2022_'))

# Multilevel Model with xGoal and SOG as predictors
model_1 = smf.mixedlm("goal ~ xGoal + shotWasOnGoal", repeated_measures_df, groups=repeated_measures_df['season_player'])
result = model_1.fit(method=['lbfgs'])
print(result.summary())

'''
model_2 = smf.mixedlm("goal ~ xGoal + shotWasOnGoal + isHomeTeam",
                      repeated_measures_df,
                      groups = 'season_team_player',
                      re_formula = '~xGoal + shotWasOnGoal',
                      vc_formula = {
                          'season': '0 + C(season_player)',
                          'team' : '0 + C(season_team)',
                          'game' : '0 + C(shot_game_ID)'
                      })
#result_2 = model_2.fit(method='lbfgs', maxiter=1000, tol=1e-6)
#print(result_2.summary())
'''
'''
fig = sns.scatterplot(x='xGoal_rolling_avg', y='goal_rolling_avg',
                data=test_filter,
                hue='season_team',
                palette='husl',
                alpha=0.5)

plt.show()
'''