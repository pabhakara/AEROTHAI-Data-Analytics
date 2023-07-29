import psycopg2
import datetime as dt
import pandas as pd

import time

from mysql.connector import Error
# Need to connect to AEROTHAI's MySQL Server

# # Try to connect to the local PostGresSQL database in which we will store our flight trajectories coupled with FPL data.
# conn_postgres = psycopg2.connect(user = "postgres",
#                                   password = "password",
#                                   host = "127.0.0.1",
#                                   port = "5432",
#                                   database = "track")

#Try to connect to the remote PostGresSQL database in which we will store our flight trajectories coupled with FPL data.
conn_postgres = psycopg2.connect(user = "de_old_data",
                                  password = "de_old_data",
                                  host = "172.16.129.241",
                                  port = "5432",
                                  database = "aerothai_dwh",
                                  options="-c search_path=dbo,public")

date_list = pd.date_range(start='2023-06-01', end='2023-06-30')

# today = dt.datetime.now()
# date_list = [dt.datetime.strptime(f"{today.year}-{today.month}-{today.day}", '%Y-%m-%d') + dt.timedelta(days=-3)]


with conn_postgres:

    cursor_postgres = conn_postgres.cursor()

    for date in date_list[:]:
        year = f"{date.year}"
        month = f"{date.month:02d}"
        day = f"{date.day:02d}"
        # Create an sql query that creates a new table for los linestring pairs in the los schema of
        # the Postgres SQL database
        postgres_sql_text = f"DROP TABLE IF EXISTS temp.cat062_{year}{month}{day}_8N; " \
                            f"SELECT s.*,'8N' " \
                            f"INTO temp.cat062_{year}{month}{day}_8N " \
                            f"FROM sur_air.cat062_{year}{month}{day} s, " \
                            f"airspace.new_bacc_sector a " \
                            f"WHERE ST_INTERSECTS(s.position,a.geom) AND " \
                            f"a.name = '8N'; "
                            # f"DROP TABLE IF EXISTS temp.cat062_{year}{month}{day}_7S; " \
                            # f"SELECT s.*,'7S' " \
                            # f"INTO temp.cat062_{year}{month}{day}_7S " \
                            # f"FROM sur_air.cat062_{year}{month}{day} s, " \
                            # f"airspace.new_bacc_sector a " \
                            # f"WHERE ST_INTERSECTS(s.position,a.geom) AND " \
                            # f"a.name = '7S'; "
 # \
 #                            f"DROP TABLE IF EXISTS temp.cat062_{year}{month}{day}_8N; " \
 #                            f"SELECT s.*,'8N' " \
 #                            f"INTO temp.cat062_{year}{month}{day}_8N " \
 #                            f"FROM sur_air.cat062_{year}{month}{day} s, " \
 #                            f"airspace.new_bacc_sector a " \
 #                            f"WHERE ST_INTERSECTS(s.position,a.geom) AND " \
 #                            f"a.name = '8N'; " \
 # \
 #                            f"DROP TABLE IF EXISTS temp.cat062_{year}{month}{day}_8S; " \
 #                            f"SELECT s.*,'8S' " \
 #                            f"INTO temp.cat062_{year}{month}{day}_8S " \
 #                            f"FROM sur_air.cat062_{year}{month}{day} s, " \
 #                            f"airspace.new_bacc_sector a " \
 #                            f"WHERE ST_INTERSECTS(s.position,a.geom) AND " \
 #                            f"a.name = '8S'; "
        print(f"working on {year}{month}{day} new_bacc_sectors")
        cursor_postgres.execute(postgres_sql_text)
        conn_postgres.commit()