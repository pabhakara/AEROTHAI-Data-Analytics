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


conn_postgres = psycopg2.connect(user="de_old_data",
                                             password="de_old_data",
                                             host="172.16.129.241",
                                             port="5432",
                                             database="aerothai_dwh")


output_filepath = '/Users/pongabha/Dropbox/Workspace/AEROTHAI Data Analytics/Flight_Proflie_Plots/'
files = glob.glob(f"{output_filepath}*")
for f in files:
    os.remove(f)

year = '2022'
month_list = ['09']
day_list = [ '01','02', '03', '04', '05','06','07','08','09','10',
                '11', '12', '13', '14', '15', ]
            # '16', '17', '18',
            #      '21', '22', '23', '24', '25','26','27','28','29','30']
#day_list = ['_monthly']
with conn_postgres:
    cursor_postgres = conn_postgres.cursor(cursor_factory=psycopg2.extras.DictCursor)
    for month in month_list:
        for day in day_list:
            postgres_sql_text = f"ALTER TABLE track.track_cat62_{year}{month}{day} " \
                                f"ADD COLUMN first_speed character varying(5);\n"\
                                f"UPDATE track.track_cat62_{year}{month}{day} t\n"\
                                f"SET first_speed = f.first_speed\n"\
                                f"FROM flight_data.flight_{year}{month} f\n"\
                                f"WHERE  t.flight_key = f.flight_key;\n"\
                                f"ALTER TABLE track.track_cat62_{year}{month}{day} " \
                                f"ADD COLUMN first_flevel character varying(5);\n"\
                                f"UPDATE track.track_cat62_{year}{month}{day} t\n"\
                                f"SET first_flevel = f.first_flevel\n"\
                                f"FROM flight_data.flight_{year}{month} f\n"\
                                f"WHERE  t.flight_key = f.flight_key;\n"
            cursor_postgres.execute(postgres_sql_text)
            conn_postgres.commit()
            print(f"Working on {year}{month}{day}")