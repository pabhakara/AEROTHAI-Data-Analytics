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
    # f"SELECT flight_key,actype, first_flevel,measured_fl, ROUND(count::numeric*5/60,2) as minutes " \
    #                 f"FROM" \
    #                 f"(" \
    #                 f"(SELECT s.flight_key, f.actype, f.first_flevel,ROUND(s.measured_fl) as measured_fl, COUNT(*) " \
    #                 f"FROM sur_air.cat062_{year}{month}{day} s, " \
    #                 f"flight_data.flight_{year}{month} f " \
    #                 f"WHERE s.flight_key = f.flight_key " \
    #                 f"AND f.dep LIKE '%' AND f.dest LIKE '%' " \
    #                 f"AND f.frule LIKE 'I' " \
    #                 f"AND (f.op_type = 'S') " \
    #                 f"AND NOT s.measured_fl IS NULL " \
    #                 f"AND NOT (LEFT(f.first_flevel,2) LIKE 'F1%' OR LEFT(f.first_flevel,1) LIKE 'S%' OR LEFT(" \
    #                 f"f.first_flevel,1) LIKE 'A%') " \
    #                 f"AND s.vert = 0 " \
    #                 f"AND s.measured_fl > 50 AND s.measured_fl < 130 " \
    #                 f"GROUP BY s.flight_key, f.actype, f.first_flevel, ROUND(s.measured_fl)) " \
    #                 f"ORDER BY s.flight_key ASC, COUNT(*) DESC" \
    #                 f") a " \
    #                 f"WHERE count > 12*3-1;"
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

date_list = pd.date_range(start='2022-05-01', end='2022-12-31', freq='D')

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
