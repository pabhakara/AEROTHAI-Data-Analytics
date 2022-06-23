import psycopg2
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

with conn_postgres:

    cursor_postgres = conn_postgres.cursor()

    year_list = ['2020','2019','2018','2017','2016','2015','2014','2013']
    month_list = ['01','02','03','04','05','06','07','08','09','10','11','12']
    
    #month_list = ['09','10','11','12']

    for year in year_list:
        for month in month_list:
            t = time.time()
            year_month = f'{year}_{month}'
            # Create an sql query that creates a new table for los linestring pairs in the los schema of 
            # the Postgres SQL database
            postgres_sql_text = f"drop table if exists los.los_{year_month}; " \
                f"select a.app_time, " \
                f"a.callsign as callsign_a, b.callsign as callsign_b, " \
                f"a.actype as actype_a, b.actype as actype_b, " \
                f"a.dep as adep_a, a.dest as ades_a, " \
                f"b.dep as adep_b, b.dest as ades_b, " \
                f"a.op_type as op_type_a, b.op_type as op_type_b, " \
                f"a.frule as frule_a, b.frule as frule_b, " \
                f"public.ST_Distance(public.ST_Transform(a.geom,3857), " \
                f"public.ST_Transform(b.geom,3857))/1852 as horizontal_separation, " \
                f"abs(a.actual_flight_level - b.actual_flight_level) as vertical_separation, " \
                f"public.ST_MakeLine(a.geom,b.geom) as geom, " \
                f"a.actual_flight_level as fl_a, b.actual_flight_level as fl_b, " \
                f"a.cdm as cdm_a, b.cdm as cdm_b, " \
                f"a.speed as speed_a, b.speed as speed_b, " \
                f"a.vx as vx_a, a.vy as vy_a, " \
                f"b.vx as vx_b, b.vy as vy_b, " \
                f"a.dist_from_last_position as dist_from_last_position_a, " \
                f"b.dist_from_last_position as dist_from_last_position_b  " \
                f"into los.los_{year_month} " \
                f"from sur_air.target_{year_month}_geom a, sur_air.target_{year_month}_geom b " \
                f"where a.app_time = b.app_time " \
                f"and abs(a.actual_flight_level - b.actual_flight_level) < 10  " \
                f"and (a.actual_flight_level > 1 and b.actual_flight_level > 1)  " \
                f"and (a.frule = 'I' or b.frule = 'I') " \
                f"and public.ST_Distance( " \
                f"public.ST_Transform(a.geom,3857), " \
                f"public.ST_Transform(b.geom,3857))/1852 < 8 " \
                f"and not(a.flight_id = b.flight_id) " \
                f"and not(a.callsign = b.callsign) " \
                f"order by app_time,horizontal_separation,vertical_separation ASC; " \
                f"DELETE FROM los.los_{year_month} a USING ( " \
                f"SELECT MIN(callsign_a) as callsign_a, app_time, horizontal_separation " \
                f"FROM los.los_{year_month}  " \
                f"GROUP BY app_time,horizontal_separation HAVING COUNT(*) > 1 " \
                f") b " \
                f"WHERE (a.app_time = b.app_time) and (a.horizontal_separation = b.horizontal_separation) " \
                f"AND a.callsign_a <> b.callsign_a;"
            print(postgres_sql_text)
            cursor_postgres.execute(postgres_sql_text)
            conn_postgres.commit()