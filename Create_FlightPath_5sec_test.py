import psycopg2
import psycopg2.extras
import datetime
import math
import time

from mysql.connector import Error

t = time.time()

# Try to connect to the local PostGresSQL database in which we will store our flight trajectories

conn_postgres = psycopg2.connect(user = "postgres",
                                  password = "password",
                                  host = "127.0.0.1",
                                  port = "5432",
                                  database = "test_db")

with conn_postgres:

    cursor_postgres = conn_postgres.cursor(cursor_factory = psycopg2.extras.DictCursor)

    # create the table name that will store the radar track
    year_month = "2021_02"
    table_name = "track_" + year_month + "_5sec"

    # Create an sql query that creates a new table for radar tracks in Postgres SQL database
    postgres_sql_text = "DROP TABLE IF EXISTS " + table_name + "; \n" + \
                        "CREATE TABLE " + table_name + " " + \
                        "(callsign character varying, _24bitaddress character(6), geom geometry, " + \
                        "start_time timestamp without time zone, " + \
                        "end_time timestamp without time zone, " + \
                        "track_id character varying, " + \
                        "dep character varying, dest character varying, " + \
                        "actype character varying)" + \
                        "WITH (OIDS=FALSE); \n" + \
                        "ALTER TABLE " + table_name + " " \
                        "OWNER TO postgres;"
    print(postgres_sql_text)
    cursor_postgres.execute(postgres_sql_text)

    conn_postgres.commit()

    # postgres_sql_text = "SELECT b.track_id,b.app_time,b._24bitaddress,b.callsign,b.dep,b.dest,b.actype," + \
    #               "b.latitude,b.longitude,b.actual_flight_level,b.cdm " + \
    #               "from target_" + year_month + "_geom b " + \
    #               "order by b.track_id,b.app_time"

    postgres_sql_text = "SELECT b.\"TID\",b.\"APPTIME\",b.\"ACADDRESS\",b.\"CALLSIGN\",b.\"DEP\",b.\"DES\",b.\"ACTYPE\"," + \
                  "b.\"LATITUDE\",b.\"LONGITUDE\",b.\"AFLEVEL\",b.\"CDM\" " + \
                  "from target_" + year_month + "_5s b " + \
                  "order by b.\"TID\",b.\"APPTIME\""

    print(postgres_sql_text)
    cursor_postgres.execute(postgres_sql_text)

    record = cursor_postgres.fetchall()

    num_of_records = len(record)
    print("num_of_record: ",num_of_records)

    k = 0

    temp_1 = record[k]
    temp_2 = record[k+1]

    app_time = str(temp_1['APPTIME'])

    _24bitaddress = str(temp_1['ACADDRESS'])
    if _24bitaddress == 'None':
        _24bitaddress = 'null'

    callsign = str(temp_1['CALLSIGN'])
    if callsign == 'None':
        callsign = 'null'

    dep = str(temp_1['DEP'])
    if dep == 'None':
        dep = 'null'

    dest = str(temp_1['DES'])
    if dest == 'None':
        dest = 'null'

    actype = str(temp_1['ACTYPE'])
    if actype == 'None':
        actype = 'null'

    track_id_1 = str(temp_1['TID'])
    track_id_2 = str(temp_2['TID'])

    postgres_sql_text = "INSERT INTO \"" + table_name + "\" (\"track_id\"," + \
                            "\"start_time\",\"geom\"," + \
                            "\"_24bitaddress\",\"callsign\",\"dep\",\"dest\",\"actype\",\"end_time\")"

    postgres_sql_text = postgres_sql_text + " VALUES('" + track_id_1 + "','" \
                    + app_time + "'," \
                    + "ST_LineFromText('LINESTRING("

    latitude_1 = str(temp_1['LATITUDE'])
    longitude_1 = str(temp_1['LONGITUDE'])

    latitude_2 = str(temp_2['LATITUDE'])
    longitude_2 = str(temp_2['LONGITUDE'])

    app_time_1 = str(temp_1['APPTIME'])

    actual_flight_level_1 = str(temp_1['AFLEVEL'])

    app_time_2 = str(temp_2['APPTIME'])

    actual_flight_level_2 = str(temp_2['AFLEVEL'])

    while k < num_of_records - 1:
        while (temp_1['TID'] == temp_2['TID']) and \
                    (temp_1['ACADDRESS'] == temp_2['ACADDRESS']) and \
                    (temp_2['APPTIME'] - temp_1['APPTIME']) <= datetime.timedelta(minutes=5) and \
                    abs(temp_2['LONGITUDE'] - temp_1['LONGITUDE']) < .1 and \
                    abs(temp_2['LATITUDE'] - temp_1['LATITUDE']) < .1:

            postgres_sql_text = postgres_sql_text + \
                                longitude_1 + " " + latitude_1 + " " + \
                                actual_flight_level_1 + ","
            k = k + 1

            if k == num_of_records-1:
                print('break \n')
                break

            temp_1 = record[k]

            latitude_1 = str(temp_1['LATITUDE'])

            longitude_1 = str(temp_1['LONGITUDE'])

            app_time_1 = str(temp_1['APPTIME'])

            #----------------
            _24bitaddress_temp = str(temp_1['ACADDRESS'])
            if not (_24bitaddress_temp == 'None'):
                _24bitaddress = _24bitaddress_temp

            callsign_temp = str(temp_1['CALLSIGN'])
            if not(callsign_temp == 'None'):
                callsign = callsign_temp

            dep_temp = str(temp_1['DEP'])
            if not(dep_temp == 'None'):
                dep = dep_temp

            dest_temp = str(temp_1['DES'])
            if not(dest_temp == 'None'):
                dest = dest_temp

            actype_temp = str(temp_1['ACTYPE'])
            if not(actype_temp == 'None'):
                actype = actype_temp

            # ----------------

            temp_2 = record[k + 1]

        postgres_sql_text = postgres_sql_text + \
                                    longitude_1 + " " + latitude_1 + " " + \
                                    actual_flight_level_1 + ","

        postgres_sql_text = postgres_sql_text + \
                                    longitude_1 + " " + latitude_1 + " " + \
                                    actual_flight_level_1 + ")',4326),'"

        postgres_sql_text = postgres_sql_text + \
                                    _24bitaddress +"','"

        postgres_sql_text = postgres_sql_text + \
                                    callsign +"','"

        postgres_sql_text = postgres_sql_text + \
                                    dep +"','"

        postgres_sql_text = postgres_sql_text + \
                                    dest +"','"

        postgres_sql_text = postgres_sql_text + \
                                    actype + "','"

        postgres_sql_text = postgres_sql_text + \
                                    app_time_1 +"')"

        cursor_postgres.execute(postgres_sql_text)
        conn_postgres.commit()

        print(str("{:.3f}".format((k / num_of_records) * 100,2)) + "% Completed")
        #print(str("{:.3f}".format((k / num_of_records) * 100,2)) + "% Completed")

        if num_of_records-k <= 2 :

            postgres_sql_text = "DROP TABLE IF EXISTS " + table_name + "_length; \n" + \
                            "SELECT *, end_time-start_time as track_duration, ST_LengthSpheroid(geom, 'SPHEROID[\"WGS 84\",6378137,298.257223563]') / 1852 as track_length " + \
                            "INTO " + table_name + "_length from " + table_name + ";" + \
                            "DROP TABLE IF EXISTS " + table_name + ";" + \
                            "ALTER TABLE " + table_name + "_length RENAME TO " + table_name + ";"

            print(postgres_sql_text)
            cursor_postgres.execute(postgres_sql_text)
            conn_postgres.commit()
            break

        k = k + 1

        temp_1 = record[k]
        temp_2 = record[k + 1]

        latitude_1 = str(temp_1['LATITUDE'])
        longitude_1 = str(temp_1['LONGITUDE'])

        latitude_2 = str(temp_2['LATITUDE'])
        longitude_2 = str(temp_2['LONGITUDE'])

        app_time_1 = str(temp_1['APPTIME'])

        actual_flight_level_1 = str(temp_1['AFLEVEL'])

        app_time_2 = str(temp_2['APPTIME'])

        actual_flight_level_2 = str(temp_2['AFLEVEL'])

        track_id_1 = str(temp_1['TID'])
        track_id_2 = str(temp_2['TID'])


        if k < num_of_records:

            postgres_sql_text = "INSERT INTO \"" + table_name + "\" (\"track_id\"," + \
                                "\"start_time\",\"geom\"," + \
                                "\"_24bitaddress\",\"callsign\",\"dep\",\"dest\",\"actype\",\"end_time\")"

            callsign = str(temp_1['CALLSIGN'])
            if callsign == 'None':
                callsign = 'null'

            app_time = str(temp_1['APPTIME'])

            _24bitaddress = str(temp_1['ACADDRESS'])
            if _24bitaddress == 'None':
                _24bitaddress = 'null'

            dep = str(temp_1['DEP'])
            if dep == 'None':
                dep = 'null'

            dest = str(temp_1['DES'])
            if dest == 'None':
                dest = 'null'

            actype = str(temp_1['ACTYPE'])
            if actype == 'None':
                actype = 'null'

            postgres_sql_text = postgres_sql_text + " VALUES('" + track_id_1 + "','" \
                                + app_time + "'," \
                                + "ST_LineFromText('LINESTRING("

        else:
            break