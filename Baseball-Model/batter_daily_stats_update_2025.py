import base64
import requests
import pandas as pd
import mysql.connector
from sqlalchemy import create_engine
from sqlalchemy import text
from ohmysportsfeedspy import MySportsFeeds
import datetime
from datetime import timedelta
from dotenv import load_dotenv
import os

load_dotenv()

key = os.getenv("MY_SPORTS_KEY")
secret = os.getenv("MY_SPORTS_SECRET")

msf = MySportsFeeds(version="1.2")
msf.authenticate(key,secret)

date = datetime.datetime(2024, 3, 18)
date_list = []

while date <= datetime.datetime.now():
    api_format = date.strftime("%Y%m%d")
    date_list.append(api_format)
    date += timedelta(days=1) 
    
df_batting = pd.DataFrame(columns=['game_date', 'player_id', 'last_name', 'first_name', 'starter_true', 'team',
                                    'AtBats','Homeruns','BatterWalks', 'BatterSwings','BatterStrikes',
                                    'BatterStrikesFoul','BatterStrikesMiss', 'BatterStrikesLooking', 'BatterGroundBalls',
                                    'BatterFlyBalls', 'BatterLineDrives','BatterStrikeouts','StolenBases',
                                    'CaughtBaseSteals','ExtraBaseHits','BatterDoublePlays','PitchesFaced',
                                    'PlateAppearances'])

for date in date_list:
    try:
        output = msf.msf_get_data(league='mlb',season='2025-regular', fordate=date, 
                                           feed='daily_player_stats', format='json')

        data = output['dailyplayerstats']['playerstatsentry']

        for each in data:
            game_date = date
            player_id = each['player']['ID']
            last_name = each['player']['LastName']
            first_name = each['player']['FirstName']
            starter_true = each['stats']['GamesStarted']['#text']
            team = each['team']['Abbreviation']
            AtBats = each['stats']['AtBats']['#text']
            Homeruns = each['stats']['Homeruns']['#text']
            BatterWalks = each['stats']['BatterWalks']['#text']
            BatterSwings = each['stats']['BatterSwings']['#text']
            BatterStrikes = each['stats']['BatterStrikes']['#text']
            BatterStrikesFoul = each['stats']['BatterStrikesFoul']['#text']
            BatterStrikesMiss = each['stats']['BatterStrikesMiss']['#text']
            BatterStrikesLooking = each['stats']['BatterStrikesLooking']['#text']
            BatterGroundBalls = each['stats']['BatterGroundBalls']['#text']
            BatterFlyBalls = each['stats']['BatterFlyBalls']['#text']
            BatterLineDrives = each['stats']['BatterLineDrives']['#text']
            BatterStrikeouts = each['stats']['BatterStrikeouts']['#text']
            StolenBases = each['stats']['StolenBases']['#text']
            CaughtBaseSteals = each['stats']['CaughtBaseSteals']['#text']
            ExtraBaseHits = each['stats']['ExtraBaseHits']['#text']
            BatterDoublePlays = each['stats']['BatterDoublePlays']['#text']
            PitchesFaced = each['stats']['PitchesFaced']['#text']
            PlateAppearances = each['stats']['PlateAppearances']['#text']
            df_batting = df_batting._append({'game_date': game_date, 'player_id': player_id, 'last_name': last_name, 
                                            'first_name': first_name, 'starter_true': starter_true, 'team': team,
                                            'AtBats': AtBats,'Homeruns': Homeruns,'BatterWalks': BatterWalks,
                                            'BatterSwings': BatterSwings,'BatterStrikes': BatterStrikes,
                                            'BatterStrikesFoul': BatterStrikesFoul,'BatterStrikesMiss': BatterStrikesMiss,
                                            'BatterStrikesLooking': BatterStrikesLooking, 'BatterGroundBalls': BatterGroundBalls,
                                            'BatterFlyBalls': BatterFlyBalls, 'BatterLineDrives': BatterLineDrives,
                                            'BatterStrikeouts': BatterStrikeouts,'StolenBases': StolenBases,
                                            'CaughtBaseSteals': CaughtBaseSteals,'ExtraBaseHits': ExtraBaseHits,
                                            'BatterDoublePlays': BatterDoublePlays,'PitchesFaced': PitchesFaced,
                                            'PlateAppearances': PlateAppearances}, ignore_index=True)
    except:
        pass
    
engine = create_engine('mysql+mysqlconnector://root:password@localhost/baseball_data')

# Use SQL query instead of read_sql_table
with engine.connect() as connection:
    existing = pd.read_sql("SELECT game_date, player_id FROM batter_daily_stats", connection)

merged = df_batting.merge(existing, on=['player_id', 'game_date'], how='left', indicator=True)
df_sql = merged[merged['_merge'] == 'left_only'].drop(columns=['_merge'])

df_sql.to_sql(name='batter_daily_stats', con=engine, if_exists='append', index=False)

