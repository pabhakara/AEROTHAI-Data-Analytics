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
        postgres_sql_text = f"DROP TABLE IF EXISTS entry_fl_and_exit_fl_{yyyymmdd}; \n" + \
                            f"CREATE TABLE entry_fl_and_exit_fl_{yyyymmdd} " + \
                            "(flight_key character varying,  " \
                            "entry_fl double precision,  " \
                            "exit_fl double precision)" + \
                            "WITH (OIDS=FALSE);"

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

            for flight_key in flight_key_list:

                # temp_1 = record[k]
                #flight_key = none_to_null(str(temp_1['flight_key']))
                #print(flight_key)

                postgres_sql_text = f"SELECT flight_key,measured_fl as entry_fl  " \
                                        f"FROM (SELECT *  " \
                                        f"	  FROM sur_air.cat062_{yyyymmdd}  " \
                                        f"	  UNION  " \
                                        f"	  SELECT *  " \
                                        f"	  FROM sur_air.cat062_{yyyymmdd_next}) x  " \
                                        f"WHERE flight_key = '{flight_key[0]}'  " \
                                        f"AND NOT sector IS NULL  " \
                                        f"ORDER BY app_time ASC  " \
                                        f"LIMIT 1"

                cursor_postgres_source.execute(postgres_sql_text)
                record_1 = cursor_postgres_source.fetchall()

                postgres_sql_text = f"SELECT flight_key,measured_fl as exit_fl  " \
                                    f"FROM (SELECT *  " \
                                    f"	  FROM sur_air.cat062_{yyyymmdd} " \
                                    f"    UNION  " \
                                    f"	  SELECT *  " \
                                    f"	  FROM sur_air.cat062_{yyyymmdd_next}) x  " \
                                    f"WHERE flight_key = '{flight_key[0]}'  " \
                                    f"AND NOT sector IS NULL  " \
                                    f"ORDER BY app_time DESC  " \
                                    f"LIMIT 1"

                cursor_postgres_source.execute(postgres_sql_text)
                record_2 = cursor_postgres_source.fetchall()


                if len(record_1) >0:
                    temp_1 = record_1[0]
                    temp_2 = record_2[0]

                    entry_fl = none_to_null(str(temp_1['entry_fl']))
                    exit_fl = none_to_null(str(temp_2['exit_fl']))

                    postgres_sql_text = f"INSERT INTO entry_fl_and_exit_fl_{yyyymmdd}  " \
                                        f"(\"flight_key\"," + \
                                        "\"entry_fl\", " \
                                        "\"exit_fl\")"

                    #print(postgres_sql_text)

                    postgres_sql_text += f" VALUES('{flight_key[0]}', " \
                                         f"{entry_fl}, " \
                                         f"{exit_fl});"

                    #print(postgres_sql_text)
                    cursor_postgres_target.execute(postgres_sql_text)
                    conn_postgres_target.commit()

                    print('track_' + yyyymmdd + str(" {:.3f}".format((k / num_of_records) * 100, 2)) + "% Completed")

                    k = k + 1