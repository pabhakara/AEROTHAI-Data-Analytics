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
conn_postgres_target = psycopg2.connect(user = "de_old_data",
                                  password = "de_old_data",
                                  host = "172.16.129.241",
                                  port = "5432",
                                  database = "aerothai_dwh",
                                  options="-c search_path=dbo,public")

# conn_postgres_target = psycopg2.connect(user = "postgres",
#                                   password = "password",
#                                   host = "127.0.0.1",
#                                   port = "5432",
#                                   database = "temp",
#                                   options="-c search_path=dbo,public")

date_list = pd.date_range(start='2022-07-01', end='2022-08-31')

with conn_postgres_target:

    cursor_postgres_target = conn_postgres_target.cursor()
    df = pd.DataFrame()

    for date in date_list:
        year = f"{date.year}"
        month = f"{date.month:02d}"
        day = f"{date.day:02d}"

        t = time.time()
        yyyymmdd = f"{year}{month}{day:}"
        yyyymm = f"{year}{month}"

        print(f"working on {yyyymmdd}")

        start_day = dt.datetime.strptime(f"{year}-{month}-{day}", '%Y-%m-%d')
        next_day = start_day + dt.timedelta(days=1)
        previous_day = start_day + dt.timedelta(days=-1)

        year_next = f"{next_day.year}"
        month_next = f"{next_day.month:02d}"
        day_next = f"{next_day.day:02d}"

        yyyymm_next = str(next_day.year).zfill(2) + str(next_day.month).zfill(2)
        yyyymm_previous = str(previous_day.year).zfill(2) + str(previous_day.month).zfill(2)

        yyyymmdd_next = str(next_day.year).zfill(2) + str(next_day.month).zfill(2) + str(next_day.day).zfill(2)
        yyyymmdd_previous = str(previous_day.year).zfill(2) + str(previous_day.month).zfill(2) + str(previous_day.day).zfill(2)

        # Create a connection to the schema in the remote PostgreSQL database
        # where the source data tables are located.
        conn_postgres_source = psycopg2.connect(user="pongabhaab",
                                     password="pongabhaab",
                                     host="172.16.129.241",
                                     port="5432",
                                     database="aerothai_dwh",
                                     options="-c search_path=dbo,sur_air")

        with conn_postgres_source:

            cursor_postgres_source = conn_postgres_source.cursor(cursor_factory=psycopg2.extras.DictCursor)

            # Create an SQL query that selects surveillance targets from the source PostgreSQL database
            postgres_sql_text =  \
            f"select flight_data.flight_{yyyymm}.dof ,flight_data.flight_{yyyymm}.acid as callsign, flight_data.flight_{yyyymm}.reg as reg, \n" \
            f"sur_air.cat062_{yyyymmdd}.icao_24bit_dap as icao_add_dap, " \
            f"flight_data.flight_{yyyymm}.icao_24bit_hex as fpl_hex, count(*) \n" \
            f"from sur_air.cat062_{yyyymmdd}, flight_data.flight_{yyyymm} \n" \
            f"where sur_air.cat062_{yyyymmdd}.flight_key = flight_data.flight_{yyyymm}.flight_key  \n" \
            f"and sur_air.cat062_{yyyymmdd}.icao_24bit_dap <> flight_data.flight_{yyyymm}.icao_24bit_hex \n" \
            f" group by dof,callsign, reg, icao_add_dap, fpl_hex; "

            cursor_postgres_source.execute(postgres_sql_text)
            record = cursor_postgres_source.fetchall()

            #print(record)
            num_of_records = len(record)

            df_temp = pd.DataFrame(record, columns=['dof','callsign','reg','icao_add_dap','fpl_hex','count'])
            df = pd.concat([df, df_temp])

    print(df)
    df.to_csv(f"/Users/pongabha/Dropbox/Workspace/Surveillance Enhancement/icao24bit_check_"
              f"{date_list[0]}_to_{date_list[-1]}.csv")

