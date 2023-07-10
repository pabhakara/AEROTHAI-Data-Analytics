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


# Create a connection to the remote PostGresSQL database in which we will store our trajectories
# created from ASTERIX Cat062 targets.
conn_postgres_target = psycopg2.connect(user="de_old_data",
                                        password="de_old_data",
                                        host="172.16.129.241",
                                        port="5432",
                                        database="aerothai_dwh",
                                        options="-c search_path=dbo,track")

# conn_postgres_target = psycopg2.connect(user = "postgres",
#                                   password = "password",
#                                   host = "127.0.0.1",
#                                   port = "5432",
#                                   database = "temp",
#                                   options="-c search_path=dbo,public")

filter = "NOT (latitude is NULL) \n" + \
         "AND NOT(flight_id is NULL) \n" + \
         "AND NOT(geo_alt < 1) \n " \
         "AND ground_speed < 700 \n " \
         "AND ground_speed > 50 \n"

date_list = pd.date_range(start='2023-07-01', end='2023-07-06')

# today = dt.datetime.now()
# date_list = [dt.datetime.strptime(f"{today.year}-{today.month}-{today.day}", '%Y-%m-%d') + dt.timedelta(days=-3)]

with conn_postgres_target:
    cursor_postgres_target = conn_postgres_target.cursor()

    for date in date_list:
        year = f"{date.year}"
        month = f"{date.month:02d}"
        day = f"{date.day:02d}"

        t = time.time()
        yyyymmdd = f"{year}{month}{day:}"
        yyyymm = f"{year}{month}"

        print(f"working on creating track_cat62_{yyyymmdd}")

        start_day = dt.datetime.strptime(f"{year}-{month}-{day}", '%Y-%m-%d')
        next_day = start_day + dt.timedelta(days=1)
        previous_day = start_day + dt.timedelta(days=-1)

        year_next = f"{next_day.year}"
        month_next = f"{next_day.month:02d}"
        day_next = f"{next_day.day:02d}"

        yyyymm_next = str(next_day.year).zfill(2) + str(next_day.month).zfill(2)
        yyyymm_previous = str(previous_day.year).zfill(2) + str(previous_day.month).zfill(2)

        yyyymmdd_next = str(next_day.year).zfill(2) + str(next_day.month).zfill(2) + str(next_day.day).zfill(2)
        yyyymmdd_previous = str(previous_day.year).zfill(2) + str(previous_day.month).zfill(2) + str(
            previous_day.day).zfill(2)

        # Create an sql query that creates a new table for radar tracks in the target PostgreSQL database
        postgres_sql_text = f"DROP TABLE IF EXISTS maintain_fl_{yyyymmdd}; \n" + \

        # print(postgres_sql_text)
        cursor_postgres_target.execute(postgres_sql_text)
        conn_postgres_target.commit()

        # Create a connection to the schema in the remote PostgreSQL database
        # where the source data tables are located.
        conn_postgres_source = psycopg2.connect(user="de_old_data",
                                                password="de_old_data",
                                                host="172.16.129.241",
                                                port="5432",
                                                database="aerothai_dwh",
                                                options="-c search_path=dbo,sur_air")

        with conn_postgres_source:

            cursor_postgres_source = conn_postgres_source.cursor(cursor_factory=psycopg2.extras.DictCursor)

            # Create an SQL query that selects surveillance targets from the source PostgreSQL database
            postgres_sql_text = f"SELECT flight_key  " \
                                f"FROM flight_data.flight_{yyyymm}  " \
                                f"WHERE flight_key LIKE '%{year}-{month}-{day}%'  " \
                                f"AND (mapped -> 'TopSky-ATC-MK-CAT062')::integer = 1  " \
                                f"ORDER BY flight_key ASC;"

            #print(postgres_sql_text)

            cursor_postgres_source.execute(postgres_sql_text)
            flight_key_list = cursor_postgres_source.fetchall()
            print(flight_key_list)
            #
            # num_of_records = len(record)
            k = 0

            num_of_records = len(flight_key_list)

            postgres_sql_text = f"SELECT x.flight_key,x.measured_fl as maintain_fl "\
                                f"INTO maitain_fl_{yyyymmdd} " \
                                    f"FROM "\
                                    f"( "\
                                    f"SELECT flight_key, measured_fl, COUNT(*) "\
                                    f"FROM "\
                                    f"(SELECT app_time,flight_key,measured_fl "\
                                    f"FROM sur_air.cat062_{yyyymmdd_next} "\
                                    f"WHERE vert = 0 AND flight_key LIKE '%{year}-{month}-{day}%' "\
                                    f"and measured_fl > 2 AND NOT sector IS NULL "\
                                    f"UNION "\
                                    f"SELECT app_time,flight_key,measured_fl "\
                                    f"FROM sur_air.cat062_{yyyymmdd} "\
                                    f"WHERE vert = 0 AND flight_key LIKE '%{year}-{month}-{day}%' "\
                                    f"and measured_fl > 2 AND NOT sector IS NULL "\
                                    f") a "\
                                    f"GROUP BY flight_key, measured_fl "\
                                    f") x, "\
                                    f"( "\
                                    f"SELECT flight_key,max(count) "\
                                    f"FROM "\
                                    f"( "\
                                    f"SELECT flight_key, measured_fl, COUNT(*) "\ 
                                    f"FROM "\
                                    f"(SELECT app_time,flight_key,measured_fl "\
                                    f"FROM sur_air.cat062_{yyyymmdd_next} "\
                                    f"WHERE vert = 0 AND flight_key LIKE '%{year}-{month}-{day}%' "\
                                    f"and measured_fl > 2 AND NOT sector IS NULL "\
                                    f"UNION "\
                                    f"SELECT app_time,flight_key,measured_fl "\
                                    f"FROM sur_air.cat062_{yyyymmdd} "\
                                    f"WHERE vert = 0 AND flight_key LIKE '%{year}-{month}-{day}%' "\
                                    f"and measured_fl > 2 AND NOT sector IS NULL "\
                                    f") a "\
                                    f"GROUP BY flight_key, measured_fl "\
                                    f"ORDER BY flight_key, count DESC "\
                                    f") x "\
                                    f"GROUP BY flight_key "\
                                    f") y "\
                                    f"WHERE x.flight_key = y.flight_key AND x.count = y.max "\
                                    f"AND x.count > 12*1 "\
                                    f"ORDER BY flight_key"
            cursor_postgres_target.execute(postgres_sql_text)
            conn_postgres_target.commit()