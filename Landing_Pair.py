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

# conn_postgres_source = psycopg2.connect(user="de_old_data",
#                                         password="de_old_data",
#                                         host="172.16.129.241",
#                                         port="5432",
#                                         database="aerothai_dwh",
#                                         options="-c search_path=dbo,public")

conn_postgres_source = psycopg2.connect(user="postgres",
                                        password="password",
                                        host="127.0.0.1",
                                        port="5432",
                                        database="temp",
                                        options="-c search_path=dbo,public")

date_list = pd.date_range(start='2024-02-07', end='2024-02-07')
with conn_postgres_source:
    cursor_postgres_source = conn_postgres_source.cursor(cursor_factory=psycopg2.extras.DictCursor)
    postgres_sql_text = ""
    for date in date_list[:]:
        year = f"{date.year}"
        month = f"{date.month:02d}"
        day = f"{date.day:02d}"
        t = time.time()
        yyyymmdd = f"{year}{month}{day:}"
        yyyymm = f"{year}{month}"
        # Create an SQL query that selects surveillance targets from the source PostgreSQL database
        # postgres_sql_text += f"DROP TABLE IF EXISTS airac_current.runways_vt_landing_buffer; " \
        #                      f"SELECT concat(airport_identifier,'-',runway_identifier),geom,ST_Buffer(geom,.002) " \
        #                      f"INTO airac_current.runways_vt_landing_buffer " \
        #                      f"FROM airac_current.runways " \
        #                      f"WHERE icao_code LIKE 'VT'; " \
        #                      f"DROP TABLE IF EXISTS sur_air.cat062_{yyyymmdd}_rwy_landing_temp; " \
        #                      f"SELECT b.concat as dest_rwy, a.* " \
        #                      f"INTO sur_air.cat062_{yyyymmdd}_rwy_landing_temp " \
        #                      f"FROM sur_air.cat062_{yyyymmdd} a, " \
        #                      f"airac_current.runways_vt_landing_buffer b " \
        #                      f"WHERE ST_INTERSECTS(b.st_buffer,a.position) " \
        #                      f"AND NOT a.flight_key IS NULL; " \
        #                      f"DROP TABLE IF EXISTS sur_air.cat062_{yyyymmdd}_rwy_landing_temp2; " \
        #                      f"SELECT * " \
        #                      f"INTO sur_air.cat062_{yyyymmdd}_rwy_landing_temp2 " \
        #                      f"FROM sur_air.cat062_{yyyymmdd}_rwy_landing_temp " \
        #                      f"WHERE concat(flight_key,app_time) IN " \
        #                      f"(SELECT concat(flight_key,min) " \
        #                      f"FROM " \
        #                      f"(SELECT flight_key,MIN(app_time) " \
        #                      f"FROM sur_air.cat062_{yyyymmdd}_rwy_landing_temp " \
        #                      f"WHERE ground_speed > 40 and vert = 2 and measured_fl < 60 " \
        #                      f"GROUP BY flight_key,app_time) a); " \
        #                      f"DROP TABLE IF EXISTS sur_air.cat062_{yyyymmdd}_rwy_landing; " \
        #                      f"SELECT * " \
        #                      f"INTO sur_air.cat062_{yyyymmdd}_rwy_landing " \
        #                      f"FROM " \
        #                      f"(SELECT concat(f.flight_key,s.app_time),f.dest as dest_fpl,f.actype, " \
        #                      f"s.* " \
        #                      f"FROM sur_air.cat062_{yyyymmdd}_rwy_landing_temp2 s, " \
        #                      f"flight_data.flight_{yyyymm} f " \
        #                      f"WHERE f.flight_key = s.flight_key) x " \
        #                      f"WHERE concat IN " \
        #                      f"(SELECT concat(flight_key,min) " \
        #                      f"FROM " \
        #                      f"(SELECT flight_key, MIN(app_time), COUNT(*) " \
        #                      f"FROM sur_air.cat062_{yyyymmdd}_rwy_landing_temp2 " \
        #                      f"GROUP BY flight_key) y); " \
        #                      f"DROP TABLE IF EXISTS sur_air.cat062_{yyyymmdd}_rwy_landing_temp; " \
        #                      f"DROP TABLE IF EXISTS sur_air.cat062_{yyyymmdd}_rwy_landing_temp2; " \
        #
        # cursor_postgres_source.execute(postgres_sql_text)
        # conn_postgres_source.commit()

        postgres_sql_text = f"SELECT dest_rwy,flight_key,app_time " \
                             f"FROM sur_air.cat062_{yyyymmdd}_rwy_landing " \
                             f"ORDER BY dest_rwy,app_time ASC " \

        cursor_postgres_source.execute(postgres_sql_text)
        record = cursor_postgres_source.fetchall()
        num_of_records = len(record)
        df = pd.DataFrame(record,columns=['dest_rwy', 'flight_key', 'app_time'])
        print(df)
        k = 0
        print(df.loc[k, 'dest_rwy'])
        while k in range(len(df)-1):
            dest_rwy_temp = df.loc[k,'dest_rwy']
            if dest_rwy_temp == df.loc[k + 1 ,'dest_rwy']:
                next_flight_key = df.loc[k + 1, 'flight_key']
                current_time = df.loc[k,'app_time']

                postgres_sql_text = f"DROP TABLE IF EXISTS sur_air.cat062_{yyyymmdd}_rwy_next_landing;" \
                                    f"SELECT concat(f.flight_key, s.app_time), f.dep as dep_fpl, f.actype, " \
                                    f"s. * " \
                                    f"FROM " \
                                    f"sur_air.cat062_20240208 s," \
                                    f"flight_data.flight_202402 f " \
                                    f"WHERE f.flight_key = s.flight_key " \
                                    f"AND s.flight_key = '{next_flight_key}' " \
                                    f"AND s.app_time = '{current_time}'"
                cursor_postgres_source.execute(postgres_sql_text)
                conn_postgres_source.commit()

                print(postgres_sql_text)
            else:
                print(dest_rwy_temp)
            k = k + 1

# path = f"/Users/pongabha/Library/CloudStorage/Dropbox/Workspace/AEROTHAI Data Analytics/TTG BKK APP/"
# filename = f"ttg_{yyyymmdd}.csv"
# df.to_csv(f"{path}{filename}")
