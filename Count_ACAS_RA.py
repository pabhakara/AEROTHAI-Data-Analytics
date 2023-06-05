import psycopg2.extras
import pandas as pd
import plotly.graph_objects as go
import datetime as dt
import dash
from dash import dcc
from dash import html
from dash.dependencies import Input, Output

import plotly
import plotly.express as px


def count_acas_ra(year, month, day):

    postgres_sql_text = f"SELECT flight_key, COUNT(*) " \
                        f"FROM sur_air.cat062_{year}{month}{day} " \
                        f"WHERE NOT acas_ra_dap IS NULL " \
                        f"GROUP BY flight_key;"

    cursor_postgres = conn_postgres.cursor()
    cursor_postgres.execute(postgres_sql_text)
    record = cursor_postgres.fetchall()
    # print(record)

    # equipage_count_temp = pd.DataFrame.from_records([record[0][0]])

    equipage_count_temp = pd.DataFrame(record)
    return equipage_count_temp


pd.options.plotting.backend = "plotly"

schema_name = 'flight_data'
conn_postgres = psycopg2.connect(user="pongabhaab",
                                 password="pongabhaab",
                                 host="172.16.129.241",
                                 port="5432",
                                 database="aerothai_dwh",
                                 options="-c search_path=dbo," + schema_name)

date_list = pd.date_range(start='2022-05-01', end='2023-05-20', freq='D')

with conn_postgres:
    equipage_count_df = pd.DataFrame()
    equipage_count_temp_4 = pd.DataFrame()
    for date in date_list:
        year = f"{date.year}"
        month = f"{date.month:02d}"
        day = f"{date.day:02d}"
        equipage_count_temp = count_acas_ra(year, month, day)
        # print(equipage_count_temp)
        equipage_count_temp_4 = pd.concat([equipage_count_temp_4, equipage_count_temp], axis=0)
        print(f"working on {year}{month}{day}")

equipage_count_df = pd.concat([equipage_count_temp_4])
# equipage_count_df = equipage_count_df.set_index(['time', 'dap'])
# print(equipage_count_df)
equipage_count_df.to_csv(f"/Users/pongabha/Desktop/num_of_acas_ra.csv")
