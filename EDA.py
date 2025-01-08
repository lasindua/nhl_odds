from io import BytesIO
import requests
import time
import pandas as pd
import zipfile
from pymongo import MongoClient
from pymongo import UpdateOne
from pymongo.server_api import ServerApi
from pymongo.errors import DuplicateKeyError
from concurrent.futures import ThreadPoolExecutor, as_completed

mongo_uri = 'mongodb+srv://lasindu:Hazelisdagoat1!@app-cluster.ckcue.mongodb.net/?retryWrites=true&w=majority&appName=app-cluster'
client = MongoClient(mongo_uri, server_api=ServerApi('1'))

db = client['NHL']

game_log_pointer = db['player_game_logs'].find({}), {'_id':0}
game_log_df = pd.DataFrame(list(game_log_pointer))

shot_pointer = db['shot_log'].find({}), {'_id':0}
shot_df = pd.DataFrame(list(shot_pointer))

print('Game Log DataFrame')
print(game_log_df.head())

print('Shot Log DataFrame')
print(shot_df.head())