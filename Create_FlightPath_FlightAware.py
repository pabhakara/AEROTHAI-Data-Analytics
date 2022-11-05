import psycopg2
import psycopg2.extras
import datetime
import math
import time
from datetime import timedelta

from mysql.connector import Error

t = time.time()

# Try to connect to the local PostGresSQL database in which we will store our flight trajectories


conn_postgres = psycopg2.connect(user = "postgres",
                                  password = "password",
                                  host = "127.0.0.1",
                                  port = "5432",
                                  database = "temp")

with conn_postgres:

    cursor_postgres = conn_postgres.cursor(cursor_factory = psycopg2.extras.DictCursor)

    # create the table name that will store the radar track
    # yyyymmdd = '20191225'
    # year_month_day = "2019_12_25"

    table_name = "track"



    # Create an sql query that creates a new table for radar tracks in Postgres SQL database
    postgres_sql_text = f"\n \n DROP TABLE IF EXISTS {table_name}; \n" + \
                                f"CREATE TABLE {table_name} " + \
                                f"(id varchar, " + \
                                f"reg varchar, " + \
                                f"dest varchar, " + \
                                f"orig varchar, " + \
                                f"type varchar, " + \
                                f"hexid varchar, " + \
                                f"start_time timestamp, " + \
                                f"end_time timestamp, " + \
                                f"ident varchar, " + \
                                f"geom geometry, " + \
                                f"adsb_version integer)" + \
                                "WITH (OIDS=FALSE); \n" + \
                                f"ALTER TABLE {table_name} " \
                                "OWNER TO postgres;"
    print(postgres_sql_text)
    cursor_postgres.execute(postgres_sql_text)

    conn_postgres.commit()

    # postgres_sql_text = "SELECT b.track_id,b.app_time,b._24bitaddress,b.callsign,b.dep,b.dest,b.actype," + \
    #               "b.latitude,b.longitude,b.actual_flight_level,b.cdm " + \
    #               "from target_" + year_month_day + "_geom b " + \
    #               "order by b.track_id,b.app_time"

    postgres_sql_text = "SELECT b.id," \
                        "b.reg," \
                        "b.dest," \
                        "b.orig," \
                        "b.type," \
                        "b.ident," \
                        "b.adsb_version," \
                        "b.hexid," \
                        "b.lat," + \
                        "b.lon," \
                        "b.alt, " + \
                        "b.time " + \
                        f"from flight_aware.position b " + \
                        f"WHERE id LIKE '%' " + \
                        "order by b.id,b.time"

    print(postgres_sql_text)
    cursor_postgres.execute(postgres_sql_text)

    record = cursor_postgres.fetchall()

    num_of_records = len(record)
    print("num_of_record: ",num_of_records)

    k = 0

    temp_1 = record[k]
    temp_2 = record[k+1]

    time = str(temp_1['time'])

    reg = str(temp_1['reg'])
    if reg == 'None':
        reg = 'null'

    dest = str(temp_1['dest'])
    if dest == 'None':
        dest = 'null'

    orig = str(temp_1['orig'])
    if orig == 'None':
        orig = 'null'

    type = str(temp_1['type'])
    if type == 'None':
        type = 'null'

    ident = str(temp_1['ident'])
    if ident == 'None':
        ident = 'null'

    adsb_version = str(temp_1['adsb_version'])

    hexid = str(temp_1['hexid'])
    if ident == 'None':
        ident = 'null'

    id_1 = str(temp_1['id'])
    id_2 = str(temp_2['id'])



    postgres_sql_text = f"INSERT INTO \"{table_name}\" " \
                         "(\"id\"," + \
                         "\"ident\"," \
                         "\"start_time\"," \
                         "\"reg\"," \
                         "\"orig\"," \
                         "\"dest\"," \
                         "\"type\"," \
                         "\"hexid\"," \
                         "\"geom\"," \
                         "\"end_time\")"

    postgres_sql_text = postgres_sql_text + " VALUES('" + id_1 + "','" \
                        + ident + "','" \
                        + time + "','" \
                        + reg + "','" \
                        + orig + "','" \
                        + dest + "','" \
                        + type + "','" \
                        + hexid + "'," \
                        + "ST_LineFromText('LINESTRING("

    latitude_1 = str(temp_1['lat'])
    longitude_1 = str(temp_1['lon'])

    latitude_2 = str(temp_2['lat'])
    longitude_2 = str(temp_2['lon'])

    time_1 = str(temp_1['time'])
    time_2 = str(temp_2['time'])

    altitude_1 = str(temp_1['alt'])
    if altitude_1 == 'None':
        altitude_1 = '-1'
    altitude_2 = str(temp_2['alt'])
    if altitude_2 == 'None':
        altitude_2 = '-1'

    while k < num_of_records - 1:
        print(f"working on {temp_1['id']}")
        while (temp_1['id'] == temp_2['id']) and \
             (temp_2['time'] - temp_1['time']) < timedelta(minutes=5):
                    # and \
                    # (temp_1['_24bitaddress'] == temp_2['_24bitaddress']) and \
                    # (temp_2['app_time'] - temp_1['app_time']) <= datetime.timedelta(minutes=5) and \
                    # abs(temp_2['longitude'] - temp_1['longitude']) < .1 and \
                    # abs(temp_2['latitude'] - temp_1['latitude']) < .1:

            postgres_sql_text = postgres_sql_text + \
                                longitude_1 + " " + latitude_1 + " " + \
                                altitude_1 + ","
            k = k + 1

            if k == num_of_records-1:
                print('break \n')
                break

            temp_1 = record[k]

            latitude_1 = str(temp_1['lat'])

            longitude_1 = str(temp_1['lon'])

            altitude_1 = str(temp_1['alt'])
            if altitude_1 == 'None':
                altitude_1 = '-1'

            time_1 = str(temp_1['time'])

            #----------------
            ident = str(temp_1['ident'])
            if ident == 'None':
                ident = 'null'

            reg = str(temp_1['reg'])
            if reg == 'None':
                reg = 'null'

            orig = str(temp_1['orig'])
            if orig == 'None':
                orig = 'null'

            dest = str(temp_1['dest'])
            if dest == 'None':
                dest = 'null'

            type = str(temp_1['type'])
            if type == 'None':
                type = 'null'

            hexid = str(temp_1['hexid'])
            if hexid == 'None':
                hexid = 'null'
            # ----------------

            temp_2 = record[k + 1]

        postgres_sql_text = postgres_sql_text + \
                                    longitude_1 + " " + latitude_1 + " " + \
                                    altitude_1 + ","

        postgres_sql_text = postgres_sql_text + \
                                    longitude_1 + " " + latitude_1 + " " + \
                                    altitude_1 + ")',4326),'"

        postgres_sql_text += time_1 + "')"

        #print(postgres_sql_text)
        cursor_postgres.execute(postgres_sql_text)
        conn_postgres.commit()

        postgres_sql_text = f"INSERT INTO \"{table_name}\" " \
                            "(\"id\"," + \
                            "\"ident\"," \
                            "\"start_time\"," \
                            "\"reg\"," \
                            "\"orig\"," \
                            "\"dest\"," \
                            "\"type\"," \
                            "\"hexid\"," \
                            "\"geom\"," \
                            "\"end_time\")"

        postgres_sql_text = postgres_sql_text + " VALUES('" + id_1 + "','" \
                            + ident + "','" \
                            + time + "','" \
                            + reg + "','" \
                            + orig + "','" \
                            + dest + "','" \
                            + type + "','" \
                            + hexid + "'," \
                            + "ST_LineFromText('LINESTRING("

        #print(postgres_sql_text)

        latitude_1 = str(temp_1['lat'])
        longitude_1 = str(temp_1['lon'])

        latitude_2 = str(temp_2['lat'])
        longitude_2 = str(temp_2['lon'])

        id_1 = str(temp_1['id'])
        id_2 = str(temp_2['id'])

        altitude_1 = str(temp_1['alt'])
        if altitude_1 == 'None':
            altitude_1 = '-1'
        altitude_2 = str(temp_2['alt'])
        if altitude_2 == 'None':
            altitude_2 = '-1'

        #cursor_postgres.execute(postgres_sql_text)

        #conn_postgres.commit()

        print(str("{:.3f}".format((k / num_of_records) * 100,2)) + "% Completed")
        #print(str("{:.3f}".format((k / num_of_records) * 100,2)) + "% Completed")

        k = k + 1
        if num_of_records - k <= 2:
            break
        temp_1 = record[k]
        temp_2 = record[k + 1]

        time = str(temp_1['time'])

        latitude_1 = str(temp_1['lat'])
        longitude_1 = str(temp_1['lon'])

        latitude_2 = str(temp_2['lat'])
        longitude_2 = str(temp_2['lon'])

        id_1 = str(temp_1['id'])
        id_2 = str(temp_2['id'])

        altitude_1 = str(temp_1['alt'])
        if altitude_1 == 'None':
            altitude_1 = '-1'
        altitude_2 = str(temp_2['alt'])
        if altitude_2 == 'None':
            altitude_2 = '-1'

        flight_id_1 = str(temp_1['id'])
        flight_id_2 = str(temp_2['id'])

        if k < num_of_records:

            postgres_sql_text = f"INSERT INTO \"{table_name}\" " \
                                "(\"id\"," + \
                                "\"ident\"," \
                                "\"start_time\"," \
                                "\"reg\"," \
                                "\"orig\"," \
                                "\"dest\"," \
                                "\"type\"," \
                                "\"hexid\"," \
                                "\"geom\"," \
                                "\"end_time\")"

            # ----------------
            ident = str(temp_1['ident'])
            if ident == 'None':
                ident = 'null'

            reg = str(temp_1['reg'])
            if reg == 'None':
                reg = 'null'

            orig = str(temp_1['orig'])
            if orig == 'None':
                orig = 'null'

            dest = str(temp_1['dest'])
            if dest == 'None':
                dest = 'null'

            type = str(temp_1['type'])
            if type == 'None':
                type = 'null'

            hexid = str(temp_1['hexid'])
            if hexid == 'None':
                hexid = 'null'

            time_1 = str(temp_1['time'])
            id_1 = str(temp_1['id'])
            # ----------------

            postgres_sql_text = postgres_sql_text + " VALUES('" + id_1 + "','" \
                                + ident + "','" \
                                + time + "','" \
                                + reg + "','" \
                                + orig + "','" \
                                + dest + "','" \
                                + type + "','" \
                                + hexid + "'," \
                                + "ST_LineFromText('LINESTRING("

        else:
            break

    schema_name = "flight_aware"

    postgres_sql_text = f"DROP TABLE IF EXISTS {schema_name}.{table_name};\n" \
                        f"ALTER TABLE public.{table_name} SET SCHEMA {schema_name};"
    print(postgres_sql_text)
    cursor_postgres.execute(postgres_sql_text)
    conn_postgres.commit()

        # postgres_sql_text = f"DROP TABLE IF EXISTS {schema_name}.{table_name}; \n" \
        #                     f"SELECT track.track_{yyyymmdd}_temp.geom," \
        #                     f"track.track_{yyyymmdd}_temp.start_time," \
        #                     f"track.track_{yyyymmdd}_temp.end_time," \
        #                     f"track.track_{yyyymmdd}_temp.track_duration," \
        #                     f"track.track_{yyyymmdd}_temp.track_length," \
        #                     f"flight_data.flight_{yyyymm}.* " \
        #                     f"INTO track.track_cat62_{yyyymmdd} " \
        #                     f"FROM track.track_{yyyymmdd}_temp " \
        #                     f"LEFT JOIN flight_data.flight_{yyyymm} ON " \
        #                     f"track.track_{yyyymmdd}_temp.flight_key = flight_data.flight_{yyyymm}.flight_key;"
        #
        # print(postgres_sql_text)
        # cursor_postgres_target.execute(postgres_sql_text)
        # conn_postgres_target.commit()