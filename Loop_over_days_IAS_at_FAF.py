import psycopg2
import psycopg2.extras
import time
import datetime as dt
import pandas as pd

import pandas as pd
from sqlalchemy import create_engine
from os import walk
import psycopg2.extras
import psycopg2

engine = create_engine('postgresql://postgres:password@localhost:5432/temp')

def none_to_null(etd):
    if etd == 'None':
        x = 'null'
    else:
        x = "'" + etd + "'"
    return x

# Create a connection to the remote PostGresSQL database from which we will retrieve our data for processing in Python

conn_postgres_source = psycopg2.connect(user = "de_old_data",
                                  password = "de_old_data",
                                  host = "172.16.129.241",
                                  port = "5432",
                                  database = "aerothai_dwh",
                                  options="-c search_path=dbo,public")

# conn_postgres_source = psycopg2.connect(user = "postgres",
#                                   password = "password",
#                                   host = "127.0.0.1",
#                                   port = "5432",
#                                   database = "temp",
#                                   options="-c search_path=dbo,public")


start_date = '2024-01-01'
end_date = '2024-04-30'

date_list = pd.date_range(start=start_date, end=end_date)

ias_df = pd.DataFrame();

postgres_sql_text = ''

with conn_postgres_source:
    cursor_postgres_source = conn_postgres_source.cursor(cursor_factory=psycopg2.extras.DictCursor)

    airport = 'VTBD'

    for date in date_list:
        year = f"{date.year}"
        month = f"{date.month:02d}"
        day = f"{date.day:02d}"

        yyyymmdd = f"{year}{month}{day:}"
        yyyymm = f"{year}{month}"

        postgres_sql_text = f"SELECT procedure_identifier, flight_key,reg, actype, " \
                            f"MIN(app_time)+(MAX(app_time)-MIN(app_time))/2 as time_at_faf," \
                            f"ROUND(AVG(ias_dap),2) as ias_dap " \
                            f"FROM " \
                            f"(SELECT b.procedure_identifier, t.flight_key,f.actype, t.app_time, t.sector, t.dist_from_last_position, " \
                            f"t.measured_fl, f.reg, " \
                            f"t.ias_dap, t.final_state_selected_alt_dap , t.latitude, t.longitude " \
                            f"FROM temp.vt_faf_buffer b, sur_air.cat062_{yyyymmdd} t " \
                            f"LEFT JOIN flight_data.flight_{yyyymm} f ON t.flight_id = f.id " \
                            f"LEFT JOIN airac_current.airports a ON a.airport_identifier = f.dest " \
                            f"WHERE f.flight_key LIKE '%' AND " \
                            f"(t.vert = 0 OR t.vert = 2) AND " \
                            f"f.dest = '{airport}' AND " \
                            f"b.procedure_identifier LIKE 'I%Z' AND " \
                            f"b.airport_identifier LIKE '{airport}' AND " \
                            f"ST_INTERSECTS(t.position, b.buffer)) x " \
                            f"GROUP BY procedure_identifier,flight_key,reg,actype;"
        cursor_postgres_source.execute(postgres_sql_text)
        record = cursor_postgres_source.fetchall()
        ias_df_temp = pd.DataFrame(record, columns=['rwy','flight_key','reg','actype','time_at_faf','ias_dap'])
        print(ias_df_temp)
        ias_df = pd.concat([ias_df, ias_df_temp], ignore_index = True)
print(ias_df)
ias_df.to_csv(f"/Users/pongabha/Dropbox/Workspace/AEROTHAI Data Analytics/Equipage Analysis/ias_at_{airport}_FAF_from_{start_date}_to_{end_date}.csv")