import psycopg2
import psycopg2.extras
import time
import datetime as dt
import pandas as pd


def none_to_null(etd):
    if etd == 'None':
        x = 'null'
    else:
        x = "'" + etd + "'"
    return x


# Create a connection to the remote PostGresSQL database from which we will retrieve our data for processing in Python

conn_postgres_source = psycopg2.connect(user="de_old_data",
                                        password="de_old_data",
                                        host="172.16.129.241",
                                        port="5432",
                                        database="aerothai_dwh",
                                        options="-c search_path=dbo,public")

# conn_postgres_source = psycopg2.connect(user = "postgres",
#                                   password = "password",
#                                   host = "127.0.0.1",
#                                   port = "5432",
#                                   database = "temp",
#                                   options="-c search_path=dbo,public")


date_list = pd.date_range(start='2025-01-21', end='2025-01-22')

with conn_postgres_source:
    cursor_postgres_source = conn_postgres_source.cursor(cursor_factory=psycopg2.extras.DictCursor)
    postgres_sql_text = ""
    for date in date_list[:-1]:
        year = f"{date.year}"
        month = f"{date.month:02d}"
        day = f"{date.day:02d}"
        t = time.time()
        yyyymmdd = f"{year}{month}{day:}"
        yyyymm = f"{year}{month}"
        # Create an SQL query that selects surveillance targets from the source PostgreSQL database
        postgres_sql_text += f"SELECT x.dof,x.acid,x.actype,x.speed_class,x.dep,x.dest,x.dest_rwy,x.sid,MIN(EXTRACT(EPOCH FROM ttg))  as ttg " \
                            f"FROM " \
                            f"(SELECT t.dof,t.acid,t.actype,s.speed_class,t.dep,t.dest,t.dest_rwy, " \
                            f"        s.waypoint_identifier as sid, s.time_of_track as time_at_sid,t.aldt,t.aldt - s.time_of_track as ttg " \
                            f"FROM " \
                            f"	(SELECT t.*,a.speed_class,b.waypoint_identifier,b.airport_identifier " \
                            f"	FROM sur_air.cat062_{yyyymmdd} t,  " \
                            f"		flight_data.flight_{yyyymm} f, " \
                            f"		temp.actype_speedclass a, " \
                            f"		(SELECT DISTINCT airport_identifier,waypoint_identifier,waypoint_latitude,waypoint_longitude,geom, ST_Buffer(geom,0.025) " \
                            f"		FROM airac_current.sids_wp " \
                            f"		WHERE waypoint_identifier IN  " \
                            f"		('BONVO','PASTO','TARED','NULNI','OLVUK','SEMBO','TL','NOBER','ALBOS','ROBKA','SELKA','LIPLI','DOSBU','GOMES','RYN','BUT','RGEOS','HHN','VANKO')) b " \
                            f"	WHERE  t.flight_id = f.id " \
                            f"	AND (f.dep LIKE 'VTBD' or f.dep LIKE 'VTBS') " \
                            f"  AND a.actype = f.actype " \
                            f"	AND ST_INTERSECTS(t.position,b.st_buffer)) s, " \
                            f"	track.track_cat62_{yyyymm} t "\
                            f"WHERE t.flight_key = s.flight_key and t.track_length > 30 " \
                            f"GROUP BY x.dof,x.acid,x.actype,x.speed_class,x.dep,x.dest,x.dest_rwy,x.sid " \
                            f"UNION "
    date = date_list[-1]
    year = f"{date.year}"
    month = f"{date.month:02d}"
    day = f"{date.day:02d}"

    yyyymmdd = f"{year}{month}{day:}"
    yyyymm = f"{year}{month}"

    postgres_sql_text += f"SELECT x.dof,x.acid,x.actype,x.speed_class,x.dep,x.dest,x.dest_rwy,x.sid,MIN(EXTRACT(EPOCH FROM ttg))  as ttg " \
                            f"FROM " \
                            f"(SELECT t.dof,t.acid,t.actype,s.speed_class,t.dep,t.dest,t.dest_rwy, " \
                            f"        s.waypoint_identifier as sid, s.time_of_track as time_at_sid,t.aldt,t.aldt - s.time_of_track as ttg " \
                            f"FROM " \
                            f"	(SELECT t.*,a.speed_class,b.waypoint_identifier,b.airport_identifier " \
                            f"	FROM sur_air.cat062_{yyyymmdd} t,  " \
                            f"		flight_data.flight_{yyyymm} f, " \
                            f"		temp.actype_speedclass a, " \
                            f"		(SELECT DISTINCT airport_identifier,waypoint_identifier,waypoint_latitude,waypoint_longitude,geom, ST_Buffer(geom,0.025) " \
                            f"		FROM airac_current.sids_wp " \
                            f"		WHERE waypoint_identifier IN  " \
                            f"		('BONVO','PASTO','TARED','NULNI','OLVUK','SEMBO','TL','NOBER','ALBOS','ROBKA','SELKA','LIPLI','DOSBU','GOMES','RYN','BUT','RGEOS','HHN','VANKO')) b " \
                            f"	WHERE  t.flight_id = f.id " \
                            f"	AND (f.dep LIKE 'VTBD' or f.dep LIKE 'VTBS') " \
                            f"  AND a.actype = f.actype " \
                            f"	AND ST_INTERSECTS(t.position,b.st_buffer)) s, " \
                            f"	track.track_cat62_{yyyymm} t "\
                            f"WHERE t.flight_key = s.flight_key and t.track_length > 30 " \
                            f"GROUP BY x.dof,x.acid,x.actype,x.speed_class,x.dep,x.dest,x.dest_rwy,x.sid"
    print(postgres_sql_text)
    cursor_postgres_source.execute(postgres_sql_text)
    record = cursor_postgres_source.fetchall()
    num_of_records = len(record)
    df = pd.DataFrame(record,columns=['dof','acid','actype','speed_class','dep','dest','dest_rwy','sid','ttg'])
    print(df)
path = f"/Users/pongabha/Library/CloudStorage/Dropbox/Workspace/AEROTHAI Data Analytics/TTG BKK APP/"
filename = f"ttg_{yyyymmdd}.csv"
df.to_csv(f"{path}{filename}")