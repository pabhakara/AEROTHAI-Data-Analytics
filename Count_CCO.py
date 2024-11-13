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


def count_cdo(year, month, day):

    postgres_sql_text = f"SELECT flight_key,actype, first_flevel,measured_fl as level_off_fl, ROUND(count::numeric*5/60,2) as duration_minutes" \
                        f", ROUND(sum::numeric,2) as distance_nm " \
                        f" FROM " \
                        f"(SELECT s.flight_key, f.actype, f.first_flevel,ROUND(s.measured_fl) as measured_fl, COUNT(*) "\
                        f"FROM sur_air.cat062_{year}{month}{day} s, "\
                        f"flight_data.flight_{year}{month} f "\
                        f"WHERE s.flight_key = f.flight_key "\
                        f"AND (f.dep LIKE 'VTBS' OR f.dep LIKE 'VTBD') "\
                        f"AND f.frule LIKE 'I' "\
                        f"AND (f.op_type = 'S') "\
                        f"AND NOT s.measured_fl IS NULL "\
                        f"AND NOT (LEFT(f.first_flevel,2) LIKE 'F1%' OR LEFT(f.first_flevel,1) LIKE 'S%' OR LEFT(f.first_flevel,1) LIKE 'A%') "\
                        f"AND s.sector LIKE 'BANGKOK%'  "\
                        f"AND s.measured_fl > 50 AND s.measured_fl < 160 "\
                        f"GROUP BY s.flight_key, f.actype, f.first_flevel, ROUND(s.measured_fl)) "\
                        f"ORDER BY s.flight_key ASC, COUNT(*) DESC "\
                        f") a " \
                        f"WHERE count > 12*1-1 "
    print(postgres_sql_text)
    cursor_postgres = conn_postgres.cursor()
    cursor_postgres.execute(postgres_sql_text)
    record = cursor_postgres.fetchall()
    # print(record)

    # equipage_count_temp = pd.DataFrame.from_records([record[0][0]])

    equipage_count_temp = pd.DataFrame(record)
    return equipage_count_temp


pd.options.plotting.backend = "plotly"

# schema_name = 'flight_data'
# conn_postgres = psycopg2.connect(user="pongabhaab",
#                                  password="pongabhaab2",
#                                  host="172.16.129.241",
#                                  port="5432",
#                                  database="aerothai_dwh",
#                                  options="-c search_path=dbo," + schema_name)

conn_postgres = psycopg2.connect(user="postgres",
                                             password="password",
                                             host="localhost",
                                             port="5432",
                                             database="temp",
                                             options="-c search_path=dbo,sur_air")



date_list = pd.date_range(start='2024-10-03', end='2024-10-03', freq='D')

with conn_postgres:
    equipage_count_df = pd.DataFrame()
    equipage_count_temp_4 = pd.DataFrame()
    for date in date_list:
        year = f"{date.year}"
        month = f"{date.month:02d}"
        day = f"{date.day:02d}"
        equipage_count_temp = count_cdo(year, month, day)
        # print(equipage_count_temp)
        equipage_count_temp_4 = pd.concat([equipage_count_temp_4, equipage_count_temp], axis=0)
        print(f"working on {year}{month}{day}")

equipage_count_df = pd.concat([equipage_count_temp_4])
# equipage_count_df = equipage_count_df.set_index(['time', 'dap'])
# print(equipage_count_df)
equipage_count_df.to_csv(f"/Users/pongabha/Library/CloudStorage/Dropbox/Workspace/CAAT/AND/KPI/KPI 2566/num_of_cco_2024-10-03.csv")
