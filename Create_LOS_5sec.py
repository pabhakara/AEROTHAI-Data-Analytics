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
                                  options="-c search_path=dbo,los")
#
#
# date_list = pd.date_range(start='2024-06-05', end='2024-06-07')

today = dt.datetime.now()
date_list = [dt.datetime.strptime(f"{today.year}-{today.month}-{today.day}", '%Y-%m-%d')
             + dt.timedelta(days=-3)]

with (conn_postgres):

    cursor_postgres = conn_postgres.cursor()

    for date in date_list[:]:
        year = f"{date.year}"
        month = f"{date.month:02d}"
        day = f"{date.day:02d}"
        # Create an sql query that creates a new table for los linestring pairs in the los schema of
        # the Postgres SQL database
        postgres_sql_text = f"DROP TABLE IF EXISTS los.los_{year}_{month}_{day}; " \
                            f"SELECT a.app_time, " \
                            f"a.flight_key as flight_key_a, b.flight_key as flight_key_b, " \
                            f"a.flight_id as flight_id_a, b.flight_id as flight_id_b, " \
                            f"public.ST_Distance( " \
                            f"public.ST_Transform(a.\"position\",3857), " \
                            "public.ST_Transform(b.\"position\",3857))/1852 as horizontal_separation, " \
                            "abs(a.measured_fl - b.measured_fl) as vertical_separation, " \
                            "public.ST_MakeLine(a.\"position\",b.\"position\") as \"geom\", " \
                            "a.measured_fl as fl_a, b.measured_fl as fl_b, " \
                            "a.ground_speed as ground_speed_a, b.ground_speed as ground_speed_b, " \
                            "a.vx as vx_a, a.vy as vy_a, " \
                            "b.vx as vx_b, b.vy as vy_b, " \
                            "a.dist_from_last_position as dist_from_last_position_a, " \
                            "b.dist_from_last_position as dist_from_last_position_b  " \
                            f"INTO los.los_{year}_{month}_{day}  " \
                            f"FROM sur_air.cat062_{year}{month}{day} a, sur_air.cat062_{year}{month}{day} b " \
                            "WHERE NOT (a.flight_key = b.flight_key) AND " \
                            "a.app_time = b.app_time " \
                            "AND abs(a.measured_fl - b.measured_fl) < 9.75  " \
                            "AND (a.measured_fl > 1 AND b.measured_fl > 10)  " \
                            "AND public.ST_Distance(public.ST_Transform(a.\"position\",3857), " \
                            "public.ST_Transform(b.\"position\",3857))/1852 < 5 " \
                            "ORDER BY flight_key_a,flight_key_b,app_time; " \
                            f"DROP TABLE IF EXISTS los.los_{year}_{month}_{day}_temp; " \
                            f"SELECT los.los_{year}_{month}_{day}.*, " \
                            "a.acid as callsign_a, b.acid as callsign_b, " \
                            "a.actype as actype_a, b.actype as actype_b, " \
                            "a.dep as adep_a, a.dest as ades_a, " \
                            "b.dep as adep_b, b.dest as ades_b, " \
                            "a.op_type as op_type_a, b.op_type as op_type_b, " \
                            "a.frule as frule_a, b.frule as frule_b " \
                            f"INTO los.los_{year}_{month}_{day}_temp  " \
                            f"FROM los.los_{year}_{month}_{day}  " \
                            f"LEFT JOIN flight_data.flight_{year}{month} a " \
                            f"ON los.los_{year}_{month}_{day}.flight_id_a = a.id " \
                            f"LEFT JOIN flight_data.flight_{year}{month} b " \
                            f"ON los.los_{year}_{month}_{day}.flight_id_b = b.id; " \
                            f"DROP TABLE IF EXISTS los.los_{year}_{month}_{day}; " \
                            f"ALTER TABLE los.los_{year}_{month}_{day}_temp " \
                            f"RENAME TO los_{year}_{month}_{day};" \
                            f"DELETE FROM los_{year}_{month}_{day} " \
                            f"WHERE (frule_a = 'V' and frule_b = 'V');" \
                            f"ALTER TABLE los.los_{year}_{month}_{day} " \
                            f"DROP COLUMN IF EXISTS time_of_los;" \
                            f"DROP TABLE IF EXISTS los.los_{year}_{month}_{day}_temp; " \
                            f"SELECT * INTO los.los_{year}_{month}_{day}_temp " \
                            f"FROM " \
                            f"( " \
                            f"    SELECT a.app_time - b.time_diff as time_of_los, a.* " \
                            f"FROM los.los_{year}_{month}_{day} a, " \
                            f"(SELECT MIN(app_time - time_of_track) as time_diff " \
                            f"FROM " \
                            f"sur_air.cat062_{year}{month}{day}) b " \
                            f") a;" \
                            f"DROP TABLE los.los_{year}_{month}_{day}; " \
                            f"ALTER TABLE los.los_{year}_{month}_{day}_temp RENAME TO los_{year}_{month}_{day}; " \
                            f"GRANT USAGE ON SCHEMA los TO ponkritsa; " \
                            f"GRANT SELECT ON ALL TABLES IN SCHEMA los TO ponkritsa; " \
                            f"GRANT USAGE ON SCHEMA los TO saifonob; " \
                            f"GRANT SELECT ON ALL TABLES IN SCHEMA los TO saifonob; " \
                            f"GRANT USAGE ON SCHEMA los TO pongabhaab; " \
                            f"GRANT SELECT ON ALL TABLES IN SCHEMA los TO pongabhaab; " \
                            f"GRANT USAGE ON SCHEMA los TO de_old_data; " \
                            f"GRANT SELECT ON ALL TABLES IN SCHEMA los TO de_old_data; "
        print(f"working on {year}{month}{day} los")
        print(postgres_sql_text)
        cursor_postgres.execute(postgres_sql_text)
        conn_postgres.commit()