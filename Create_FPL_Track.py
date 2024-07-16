import datetime as dt
import time
from subprocess import PIPE, Popen

import pandas as pd

import psycopg2.extras

import psycopg2

def none_to_null(etd):
    if etd == 'None':
        x = 'null'
    else:
        x = "'" + etd + "'"
    return x

conn_postgres_source = psycopg2.connect(user="postgres",
                                        password="password",
                                        host="127.0.0.1",
                                        port="5432",
                                        database="temp",
                                        options="-c search_path=dbo,temp")

conn_postgres_target = psycopg2.connect(user="postgres",
                                        password="password",
                                        host="127.0.0.1",
                                        port="5432",
                                        database="temp",
                                        options="-c search_path=dbo,public")

with (conn_postgres_source):
    with (conn_postgres_target):
        cursor_postgres_target = conn_postgres_target.cursor()
        # Create an sql query that creates a new table for radar tracks in the target PostgreSQL database
        postgres_sql_text = f"DROP TABLE IF EXISTS fpl_track_mapping; \n" + \
                            f"CREATE TABLE fpl_track_mapping " + \
                            "(fdmc_bkk_fix text [], num_of_tracks integer, " \
                            "geom geometry)" + \
                            "WITH (OIDS=FALSE);"

        # print(postgres_sql_text)
        cursor_postgres_target.execute(postgres_sql_text)
        conn_postgres_target.commit()

        cursor_postgres_source = conn_postgres_source.cursor(cursor_factory=psycopg2.extras.DictCursor)
        # Create an SQL query that selects surveillance targets from the source PostgreSQL database
        postgres_sql_text = f"SELECT fdmc_bkk_fix,count " \
                            f"FROM " \
                            f"(SELECT fdmc_bkk_fix,COUNT(*) " \
                            f" FROM flight_data.flight_2023 " \
                            f" WHERE NOT fdmc_bkk_fix IS NULL " \
                            f" AND NOT dep = dest " \
                            f" AND frule = 'I'" \
                            f" AND NOT 'AGG' = ANY(fdmc_bkk_fix)" \
                            f" AND NOT '1429N10334E' = ANY(fdmc_bkk_fix)" \
                            f" AND NOT '1331N10040E' = ANY(fdmc_bkk_fix)" \
                            f" AND NOT '1249N10118E' = ANY(fdmc_bkk_fix)" \
                            f" AND NOT '1246N10058E' = ANY(fdmc_bkk_fix)" \
                            f" AND NOT '1235N10059E' = ANY(fdmc_bkk_fix)" \
                            f" AND NOT '1203N10112E' = ANY(fdmc_bkk_fix)" \
                            f" AND NOT 'SURIN' = ANY(fdmc_bkk_fix)" \
                            f" AND NOT 'SIHANOUK' = ANY(fdmc_bkk_fix)" \
                            f" AND NOT 'VVZ' = ANY(fdmc_bkk_fix)" \
                            f" GROUP BY fdmc_bkk_fix " \
                            f" ORDER BY COUNT(*) DESC) a;"

        print(postgres_sql_text)

        cursor_postgres_source.execute(postgres_sql_text)
        FPL_list = cursor_postgres_source.fetchall()

        num_of_FPLs = len(FPL_list)

        print(num_of_FPLs)

        for m in range(num_of_FPLs):

            print(m)

            waypoint_list = FPL_list[m][0]
            num_of_tracks = FPL_list[m][1]

            waypoint_array = '{' + ', '.join([str(x) for x in waypoint_list]) + '}'

            postgres_sql_insert = f"INSERT INTO \"fpl_track_mapping\" (\"fdmc_bkk_fix\"," + \
                                f"\"num_of_tracks\",\"geom\") VALUES('{waypoint_array}',{num_of_tracks},"

            num_of_waypoints = len(waypoint_list)
            print(num_of_waypoints)
            if num_of_waypoints > 1:
                LineString_Text = "ST_LineFromText('LINESTRING("

                k = 0

                postgres_sql_text = f"SELECT DISTINCT identifier, longitude, latitude " \
                                    f"FROM public.waypoint_2023" \
                                    f" WHERE identifier = '{waypoint_list[k]}' "
                print(postgres_sql_text)
                cursor_postgres_source.execute(postgres_sql_text)

                record = cursor_postgres_source.fetchall()
                if len(record) > 0:
                    waypoint = record[0]
                    #print(waypoint)
                    longitude = waypoint[1]
                    latitude = waypoint[2]
                    LineString_Text += f"{longitude} {latitude} {0} , "
                    #print(LineString_Text)

                k = 1

                while k < (num_of_waypoints) - 1:
                    postgres_sql_text = f"SELECT DISTINCT identifier, longitude, latitude " \
                                        f"FROM public.waypoint_2023" \
                                        f" WHERE identifier = '{waypoint_list[k]}' "
                    cursor_postgres_source.execute(postgres_sql_text)
                    record = cursor_postgres_source.fetchall()
                    if len(record) > 0:
                        waypoint = record[0]
                        #print(waypoint)
                        longitude = waypoint[1]
                        latitude = waypoint[2]
                        LineString_Text += f"{longitude} {latitude} {0} , "
                        #print(LineString_Text)

                    k = k + 1

                postgres_sql_text = f"SELECT DISTINCT identifier, longitude, latitude " \
                                    f"FROM public.waypoint_2023" \
                                    f" WHERE identifier = '{waypoint_list[k]}' "

                cursor_postgres_source.execute(postgres_sql_text)
                record = cursor_postgres_source.fetchall()
                waypoint = record[0]
                longitude = waypoint[1]
                latitude = waypoint[2]
                LineString_Text += f"{longitude} {latitude} {0} )') "
                print(LineString_Text)
                postgres_sql_insert = postgres_sql_insert + LineString_Text + ");"
                cursor_postgres_target.execute(postgres_sql_insert)
                conn_postgres_target.commit()
