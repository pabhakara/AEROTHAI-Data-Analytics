import psycopg2
import psycopg2.extras
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.ticker import (AutoMinorLocator, MultipleLocator)
from matplotlib.dates import DateFormatter
import os
import glob

import plotly.express as px
from plotly.subplots import make_subplots
import plotly.graph_objects as go

from dash import Dash, html, dcc
import time
import datetime as dt
def none_to_null(etd):
    if etd == 'None':
        x = 'null'
    else:
        x = "'" + etd + "'"
    return x


conn_postgres_source = psycopg2.connect(user="pongabhaab",
                                             password="pongabhaab2",
                                             host="172.16.129.241",
                                             port="5432",
                                             database="aerothai_dwh",
                                             options="-c search_path=dbo,sur_air")


output_filepath = '/Users/pongabha/Dropbox/Workspace/AEROTHAI Data Analytics/Flight_Proflie_Plots/'
files = glob.glob(f"{output_filepath}*")
for f in files:
    os.remove(f)

# year = '2022'
# month = '08'
# day_list = [ '01','02', '03', '04', '05','06','07','08','09','10',
#                 '11', '12', '13', '14', '15', '16', '17', '18','19','20'
#              '21', '22', '23', '24', '25','26','27','28','29','30','31']
#
summary_df = pd.DataFrame()

date_list = pd.date_range(start='2024-09-01', end='2024-09-22')

with conn_postgres_source:
    cursor_postgres_source = conn_postgres_source.cursor(cursor_factory=psycopg2.extras.DictCursor)
    for date in date_list:
        year = date.year
        month = date.month
        day = date.day
        postgres_sql_text = f"SELECT left(f.acid,3) as operator,f.actype, f.reg, f.icao_24bit_hex,f.dof, COUNT(*) "\
                f"FROM sur_air.cat062_{year}{month:02d}{day:02d} t, flight_data.flight_{year}{month:02d} f "\
                f"WHERE f.reg IN "\
                f"(SELECT DISTINCT a.reg "\
                f"FROM "\
                f"(SELECT left(acid,3) AS operator, reg, actype, dest, (sur like '%B%') AS adsb_equipped, COUNT(*) "\
                f"FROM flight_data.flight_{year}{month:02d} "\
                f"WHERE frule like 'I' and (sur like '%B%') is false and dof = '{year}-{month:02d}-{day:02d}' "\
                f"GROUP BY left(acid,3),reg, actype, dest,(sur like '%B%') "\
                f"ORDER BY count DESC) a) "\
                f"AND f.flight_key = t.flight_key "\
                f"AND "\
                f"(51 = ANY(t.contributing_sensors) "\
                f"OR 52 = ANY(t.contributing_sensors) "\
                f"OR 53 = ANY(t.contributing_sensors) "\
                f"OR 54 = ANY(t.contributing_sensors) "\
                f"OR 55 = ANY(t.contributing_sensors) "\
                f"OR 56 = ANY(t.contributing_sensors) "\
                f"OR 83 = ANY(t.contributing_sensors) "\
                f"OR 100 = ANY(t.contributing_sensors) "\
                f"OR 147 = ANY(t.contributing_sensors)) "\
                f"GROUP BY left(f.acid,3),f.actype, f.reg, f.icao_24bit_hex,f.dof "
        cursor_postgres_source.execute(postgres_sql_text)
        print(postgres_sql_text)

        record = cursor_postgres_source.fetchall()
        print(record)
        df_temp = pd.DataFrame(record, columns=['operator','actype','reg','icao_24bit_hex','dof','count'])
        summary_df = pd.concat([summary_df, df_temp])
        print(date)
print(summary_df)
path = f"/Users/pongabha/Dropbox/Workspace/AEROTHAI Data Analytics/Equipage Analysis/"
summary_df.to_csv(f"{path}Non_ADS-B_equipped_with_ADS-B_targets_{year}{month:02d}.csv")