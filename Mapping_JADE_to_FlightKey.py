import psycopg2
import psycopg2.extras
import time
import datetime as dt
import pandas as pd


# Create a connection to the remote PostGresSQL database in which we will store our trajectories
# created from ASTERIX Cat062 targets.
# conn_postgres_target = psycopg2.connect(user = "de_old_data",
#                                   password = "de_old_data",
#                                   host = "172.16.129.241",
#                                   port = "5432",
#                                   database = "aerothai_dwh",
#                                   options="-c search_path=dbo,public")

conn_postgres_target = psycopg2.connect(user = "postgres",
                                  password = "password",
                                  host = "127.0.0.1",
                                  port = "5432",
                                  database = "temp",
                                  options="-c search_path=dbo,public")


jade_table_list = ["local_flight_plan","handoffs","controlled_aircraft_periods"]

date_list = pd.date_range(start='2022-05-01', end='2022-10-25')

with conn_postgres_target:

    for jade_table in jade_table_list:
        print(f"working on {jade_table}")
        cursor_postgres_target = conn_postgres_target.cursor()
        postgres_sql_text = f"DROP TABLE IF EXISTS temp.{jade_table}_flightkey; \n" \
                            f"SELECT * \n" \
                            f"INTO temp.{jade_table}_flightkey \n" \
                            f"FROM ("

        for date in date_list[:-1]:
            year = f"{date.year}"
            month = f"{date.month:02d}"
            day = f"{date.day:02d}"

            yyyymmdd = f"{year}{month}{day:}"
            yyyymm = f"{year}{month}"

            #print(f"working on {yyyymmdd}")

            postgres_sql_text += f"SELECT f.flight_key,l.* " \
                                f"FROM jade_mk.{jade_table} l, \n" \
                                f"flight_data.flight_{yyyymm} f \n" \
                                f"WHERE l.callsign = f.acid \n" \
                                f"AND l.dof = '22{month}{day}' \n" \
                                f"AND f.dep = l.adep \n" \
                                f"AND f.dest = l.ades \n" \
                                f"AND extract(day from f.dof) = {day} \n" \
                                f"AND (extract(hour from f.eobt) = extract(hour from l.eobt))  \n" \
                                f"AND f.acid = l.callsign \n" \
                                f"AND f.flight_key IN \n" \
                                f"(SELECT flight_key \n" \
                                f"FROM flight_data.flight_{yyyymm} \n" \
                                f"WHERE \n" \
                                f"	concat(acid,dep,dest,dof) \n" \
                                f"IN	 \n" \
                                f"(SELECT key \n" \
                                f"FROM \n" \
                                f"	(SELECT concat(acid,dep,dest,dof) as key,COUNT(*) \n" \
                                f"	FROM flight_data.flight_{yyyymm}	 \n" \
                                f"	WHERE extract(day from dof) = {day} \n" \
                                f"	GROUP BY acid,dep,dest,dof) a \n" \
                                f"WHERE count > 0)) \n" \
                                "UNION \n"
            #print(postgres_sql_text)
        date = date_list[-1]
        year = f"{date.year}"
        month = f"{date.month:02d}"
        day = f"{date.day:02d}"

        yyyymmdd = f"{year}{month}{day:}"
        yyyymm = f"{year}{month}"


        postgres_sql_text += f"SELECT f.flight_key,l.* \n" \
                             f"FROM jade_mk.{jade_table} l, \n" \
                             f"flight_data.flight_{yyyymm} f \n" \
                             f"WHERE l.callsign = f.acid \n" \
                             f"AND l.dof = '22{month}{day}' \n" \
                             f"AND f.dep = l.adep \n" \
                             f"AND f.dest = l.ades \n" \
                             f"AND extract(day from f.dof) = {day} \n" \
                             f"AND (extract(hour from f.eobt) = extract(hour from l.eobt))  \n" \
                             f"AND f.acid = l.callsign \n" \
                             f"AND f.flight_key IN \n" \
                             f"(SELECT flight_key \n" \
                             f"FROM flight_data.flight_202208 \n" \
                             f"WHERE \n" \
                             f"	concat(acid,dep,dest,dof) \n" \
                             f"IN	 \n" \
                             f"(SELECT key \n" \
                             f"FROM \n" \
                             f"	(SELECT concat(acid,dep,dest,dof) as key,COUNT(*) \n" \
                             f"	FROM flight_data.flight_{yyyymm}	 \n" \
                             f"	WHERE extract(day from dof) = {day} \n" \
                             f"	GROUP BY acid,dep,dest,dof) a \n" \
                             f" WHERE count > 0))) b"
        #print(postgres_sql_text)
        cursor_postgres_target.execute(postgres_sql_text)
        conn_postgres_target.commit()


