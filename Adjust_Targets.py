import psycopg2
import datetime
import mysql.connector
import math
import time

from mysql.connector import Error

t = time.time()

# Try to connect to the local PostGresSQL database in which we will store our flight trajectories coupled with FPL data.
try:
    conn_postgres = psycopg2.connect(user = "postgres",
                                  password = "password",
                                  host = "127.0.0.1",
                                  port = "5432",
                                  database = "flight_postgres")

    cursor_postgres = conn_postgres.cursor()

# create the table name that will store the radar track
    year_month = "2019_12"
    table_name = "target_" + year_month + "_adjusted"

    # center_map_latitude = 13.89
    # center_map_longitude = 100.6
    # lat_offset_scale = 0
    # lon_offset_scale = 0

    Re = 6371  # Earth Radius in km

    # radar target positions (lat, long) adjustment parameters
    center_map_latitude = 13
    center_map_longitude = 101.3
    lat_offset_scale = .21
    lon_offset_scale = .3

    # Create an sql query that creates a new table for radar tracks in Postgres SQL database
    postgres_sql_text = "DROP TABLE IF EXISTS " + table_name + "; \n" + \
                        "CREATE TABLE " + table_name + " " + \
                        "(callsign character varying, flight_id integer, geom geometry, " + \
                        "start_time timestamp without time zone, " + \
                        "etd timestamp without time zone, " + \
                        "atd timestamp without time zone, " + \
                        "eta timestamp without time zone, " + \
                        "ata timestamp without time zone, " + \
                        "end_time timestamp without time zone, " + \
                        "dep character varying, dest character varying, " + \
                        "actype character varying, " + \
                        "runway character varying, " + \
                        "sidstar character varying, " + \
                        "op_type character varying, " + \
                        "frule character varying, " + \
                        "reg character varying, " + \
                        "pbn_type character varying, " + \
                        "entry_flevel integer, " + \
                        "maintain_flevel integer, " + \
                        "exit_flevel integer, " + \
                        "comnav character varying, flevel integer,route character varying)" + \
                        "WITH (OIDS=FALSE); \n" + \
                        "ALTER TABLE " + table_name + " " \
                        "OWNER TO postgres;"
    print(postgres_sql_text)
    cursor_postgres.execute(postgres_sql_text)

    conn_postgres.commit()

    offset_1 = (Re ** 2 + ((center_map_latitude - float(temp_1['latitude'])) * 60 * 1.852) ** 2) ** 0.5 - Re
    offset_lat_1 = offset_1 * 1.852 / 60. * math.sin(
        math.radians((float(temp_1['latitude'])) - center_map_latitude)) * lat_offset_scale
    latitude_1 = str(float(temp_1['latitude']) + offset_lat_1)


    offset_4 = (Re ** 2 + ((center_map_longitude - float(temp_2['longitude'])) * 60 * 1.852) ** 2) ** 0.5 - Re
    offset_lon_2 = offset_4 * 1.852 / 60. * math.sin(
        math.radians((float(temp_2['longitude'])) - center_map_longitude)) * lon_offset_scale
    longitude_2 = str(float(temp_1['longitude']) + offset_lon_2)


except (Exception, psycopg2.Error) as error :
    print ("Error while connecting to PostgreSQL", error)
finally:
    #closing database connection.
        if conn_postgres:
            cursor_postgres.close()
            conn_postgres.close()
            print("PostgreSQL connection is closed")
            elapsed = time.time() - t
            print('Elapsed: %s sec' % elapsed)