from prefect import flow, task
import os
import pandas as pd
import datetime
from datetime import timedelta
from sqlalchemy import create_engine
from dotenv import load_dotenv
from ohmysportsfeedspy import MySportsFeeds
import mysql.connector

load_dotenv()

@task
def fetch_batting_data():
    key = os.getenv("MY_SPORTS_KEY")
    secret = os.getenv("MY_SPORTS_SECRET")

    msf = MySportsFeeds(version="1.2")
    msf.authenticate(key, secret)

    date = datetime.datetime(2024, 3, 18)
    date_list = []
    while date <= datetime.datetime.now():
        date_list.append(date.strftime("%Y%m%d"))
        date += timedelta(days=1)

    df_batting = pd.DataFrame(columns=[...])  # Keep your full column list here

    for date in date_list:
        try:
            output = msf.msf_get_data(league='mlb', season='2025-regular', fordate=date, feed='daily_player_stats', format='json')
            data = output['dailyplayerstats']['playerstatsentry']
            for each in data:
                # extract and append rows — same logic as before
                ...
        except:
            pass

    return df_batting

@task
def update_mysql(df_batting):
    engine = create_engine('mysql+mysqlconnector://root:password@localhost/baseball_data')
    with engine.connect() as connection:
        existing = pd.read_sql("SELECT game_date, player_id FROM batter_daily_stats", connection)

    merged = df_batting.merge(existing, on=['player_id', 'game_date'], how='left', indicator=True)
    df_sql = merged[merged['_merge'] == 'left_only'].drop(columns=['_merge'])

    df_sql.to_sql(name='batter_daily_stats', con=engine, if_exists='append', index=False)

@flow
def batter_daily_flow():
    df = fetch_batting_data()
    update_mysql(df)

if __name__ == "__main__":
    batter_daily_flow()
