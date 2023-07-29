import psycopg2
import psycopg2.extras
import time
import datetime as dt
import pandas as pd

conn_postgres_target = psycopg2.connect(user="postgres",
                                        password="password",
                                        host="127.0.0.1",
                                        port="5432",
                                        database="temp",
                                        options="-c search_path=dbo,public")

conn_postgres_source = psycopg2.connect(user="postgres",
                                        password="password",
                                        host="127.0.0.1",
                                        port="5432",
                                        database="temp",
                                        options="-c search_path=dbo,sur_air")

with conn_postgres_target:
    cursor_postgres_target = conn_postgres_target.cursor()
    postgres_sql_text = f"DROP TABLE IF EXISTS cat062_20230627_noadsb_1sec; \n" \
                        f"CREATE TABLE cat062_20230627_noadsb_1sec \n" \
                        f"(flight_key character varying, \n" \
                        f"track_no integer, \n" \
                        f"time_of_track timestamp without time zone, \n" \
                        f"geom geometry) \n" \
                        f"WITH (OIDS=FALSE);"
    # print(postgres_sql_text)
    cursor_postgres_target.execute(postgres_sql_text)
    conn_postgres_target.commit()

with conn_postgres_source:
    cursor_postgres_source = conn_postgres_source.cursor(cursor_factory=psycopg2.extras.DictCursor)

    # postgres_sql_text = f"SELECT flight_key FROM sur_air.cat062_20230627_noadsb \n" \
    #                     f"WHERE NOT flight_key IS NULL \n" \
    #                     f"GROUP BY flight_key;"

    postgres_sql_text = f"SELECT flight_key,track_no \n" \
                        f"FROM sur_air.cat062_20230627_noadsb \n" \
                        f"WHERE flight_key LIKE '%' \n" \
                        f"GROUP BY flight_key,track_no;"

    cursor_postgres_source.execute(postgres_sql_text)
    flight_key_track_no_list = cursor_postgres_source.fetchall()

    for flight_key_track_no in flight_key_track_no_list:
        flight_key = flight_key_track_no[0]
        print(flight_key)
        #print(flight_key)
        track_no = flight_key_track_no[1]
        postgres_sql_text = f"(SELECT time_of_track,latitude,longitude \n" \
                            f"FROM sur_air.cat062_20230627_noadsb \n" \
                            f"WHERE flight_key = '{flight_key}' AND track_no = {track_no} \n" \
                            f"ORDER BY time_of_track, time_of_track);"
        #print(postgres_sql_text)
        cursor_postgres_source.execute(postgres_sql_text)
        record = cursor_postgres_source.fetchall()
        x = len(record)
        df = pd.DataFrame(record, columns=[['time_of_track', 'latitude', 'longitude']])
        #print(df)
        k = 0
        while k < x - 1:
            #print(k)
            temp = df[k:k + 2]
            time_gap = int((temp['time_of_track'].iloc[1] - temp['time_of_track'].iloc[0]).astype(int) / 10 ** 9)
            latitude_gap = (temp['latitude'].iloc[1] - temp['latitude'].iloc[0])
            longitude_gap = (temp['longitude'].iloc[1] - temp['longitude'].iloc[0])
            #print(f"latitude_gap: {latitude_gap}")
            # print(time_gap)
            m = 0
            for m in range(time_gap):
                time = ((temp['time_of_track'].iloc[0] + pd.DateOffset(seconds=m)).iloc[0])
                latitude = ((temp['latitude'].iloc[0] + latitude_gap / time_gap * m).iloc[0])
                longitude = ((temp['longitude'].iloc[0] + longitude_gap / time_gap * m).iloc[0])
                postgres_sql_text = f"INSERT INTO \"cat062_20230627_noadsb_1sec\" (\"flight_key\"," \
                                    f"\"track_no\"," \
                                    f"\"time_of_track\"," \
                                    f"\"geom\")"

                postgres_sql_text += f" VALUES('{flight_key}'," \
                                     f"{track_no}," \
                                     f"'{time}'," \
                                     f"ST_SetSRID(ST_MakePoint({longitude}, {latitude}),4326));"

                cursor_postgres_target.execute(postgres_sql_text)
                conn_postgres_target.commit()
            k = k + 1

with conn_postgres_target:
    cursor_postgres_target = conn_postgres_target.cursor()
    postgres_sql_text = f"DROP TABLE IF EXISTS sur_air.cat062_20230627_noadsb_1sec;\n" \
                        f"ALTER TABLE public.cat062_20230627_noadsb_1sec SET SCHEMA sur_air;"
    print(postgres_sql_text)
    cursor_postgres_target.execute(postgres_sql_text)
    conn_postgres_target.commit()