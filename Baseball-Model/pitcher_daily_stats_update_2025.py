import base64
import requests
import pandas as pd
import mysql.connector
from sqlalchemy import create_engine
from sqlalchemy import text
from ohmysportsfeedspy import MySportsFeeds
from dotenv import load_dotenv
import os

load_dotenv()

key = os.getenv("MY_SPORTS_KEY")
secret = os.getenv("MY_SPORTS_SECRET")

msf = MySportsFeeds(version="1.2")
msf.authenticate(key,secret)

import datetime
from datetime import timedelta 

date = datetime.datetime(2025, 3, 18)
date_list = []

while date <= datetime.datetime.now():
    api_format = date.strftime("%Y%m%d")
    date_list.append(api_format)
    date += timedelta(days=1) 
    
df_pitcher = pd.DataFrame(columns=['game_date', 'player_id', 'last_name', 'first_name', 'starter_true',
                                  'InningsPitched', 'HitsAllowed', 'EarnedRunsAllowed','PitcherWalks', 'team',
                                   'PitcherSwings', 'PitcherStrikes','PitcherStrikeouts'])

for date in date_list:
    try:
        output = msf.msf_get_data(league='mlb',season='2025-regular', fordate=date, 
                                   feed='daily_player_stats', position='P', format='json')

        data = output['dailyplayerstats']['playerstatsentry']

        for each in data:
            game_date = date
            player_id = each['player']['ID']
            last_name = each['player']['LastName']
            first_name = each['player']['FirstName']
            team = each['team']['Abbreviation']
            starter_true = each['stats']['GamesStarted']['#text']
            InningsPitched = each['stats']['InningsPitched']['#text']
            HitsAllowed = each['stats']['HitsAllowed']['#text']
            EarnedRunsAllowed = each['stats']['EarnedRunsAllowed']['#text']
            PitcherWalks = each['stats']['PitcherWalks']['#text']
            PitcherSwings = each['stats']['PitcherSwings']['#text']
            PitcherStrikes = each['stats']['PitcherStrikes']['#text']
            PitcherStrikeouts = each['stats']['PitcherStrikeouts']['#text']
            #game_id = [each][0]['scheduled']
            df_pitcher = df_pitcher._append({'game_date': game_date, 'player_id': player_id, 'last_name': last_name, 
                                             'first_name': first_name, 'starter_true': starter_true, 
                                             'InningsPitched': InningsPitched, 'HitsAllowed': HitsAllowed, 
                                             'EarnedRunsAllowed': EarnedRunsAllowed, 'PitcherWalks': PitcherWalks, 
                                             'PitcherSwings': PitcherSwings,'PitcherStrikes': PitcherStrikes, 
                                             'PitcherStrikeouts': PitcherStrikeouts, 'team': team}, ignore_index=True)
    except: 
        pass

df_pitcher2 = df_pitcher[(df_pitcher['starter_true'] == '1') & (df_pitcher['InningsPitched'] > '0')].reset_index(drop=True)
df_pitcher2.reset_index(drop=True, inplace=True)

engine = create_engine('mysql+mysqlconnector://root:password@localhost/baseball_data')

#connection = engine.connect()

# Create SQLAlchemy engine
engine = create_engine('mysql+mysqlconnector://root:password@localhost/baseball_data')

# Use SQL query instead of read_sql_table
with engine.connect() as connection:
    existing = pd.read_sql("SELECT game_date, player_id FROM pitcher_daily_stats", connection)

merged = df_pitcher2.merge(existing, on=['player_id', 'game_date'], how='left', indicator=True)
df_sql = merged[merged['_merge'] == 'left_only'].drop(columns=['_merge'])

#df_sql.head(25)

df_sql.to_sql(name='pitcher_daily_stats', con=engine, if_exists='append', index=False)