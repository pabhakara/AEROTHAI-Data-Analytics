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
                                  database = "aireon")

with conn_postgres:

    cursor_postgres = conn_postgres.cursor(cursor_factory = psycopg2.extras.DictCursor)

    # create the table name that will store the radar track

    table_name = f"aireon_2023_04_{day}"

    year = '2023'
    month = '04'
    day_list = ['14', '15']

    for day in day_list:

        yyyymmdd = f'{year}{month}{day}'
        year_month_day = f"{year}_{month}_{day}"

        table_name = "track_" + year_month_day + "_aireon"

        # Create an sql query that creates a new table for radar tracks in Postgres SQL database
        postgres_sql_text = f"\n \n DROP TABLE IF EXISTS {table_name}; \n" + \
                                    f"CREATE TABLE {table_name} " + \
                                    "(flight_id character varying, " \
                                    "geom geometry, " + \
                                    "aircraft_id character varying, " + \
                                    "callsign character varying, " + \
                                    "start_time timestamp, " + \
                                    "end_time timestamp)" + \
                                    "WITH (OIDS=FALSE); \n" + \
                                    f"ALTER TABLE {table_name} " \
                                    "OWNER TO postgres;"
        print(postgres_sql_text)
        cursor_postgres.execute(postgres_sql_text)

        conn_postgres.commit()

        postgres_sql_text = "SELECT b.flight_id," \
                            "b.aircraft_id," \
                            "b.snapshot_id," \
                            "b.latitude," \
                            "b.longitude," \
                            "b.altitude," \
                            "b.reg," \
                            "b.equip," + \
                            "b.callsign," \
                            "b.equip," + \
                            "b.schd_from," + \
                            "b.schd_to " + \
                            f"from position_{yyyymmdd} b " + \
                            "order by b.flight_id,b.snapshot_id"

        print(postgres_sql_text)
        cursor_postgres.execute(postgres_sql_text)

        record = cursor_postgres.fetchall()

        num_of_records = len(record)
        print("num_of_record: ",num_of_records)

        k = 0

        temp_1 = record[k]
        temp_2 = record[k+1]

        snapshot_id_1 = str(temp_1['snapshot_id'])

        aircraft_id = str(temp_1['aircraft_id'])
        if aircraft_id == 'None':
            aircraft_id = 'null'

        callsign = str(temp_1['callsign'])
        if callsign == 'None':
            callsign = 'null'

        reg = str(temp_1['reg'])
        if callsign == 'None':
            callsign = 'null'

        schd_from = str(temp_1['schd_from'])
        if schd_from == 'None':
            schd_from = 'null'

        schd_to = str(temp_1['schd_to'])
        if schd_to == 'None':
            schd_to = 'null'

        equip = str(temp_1['equip'])
        if equip == 'None':
            equip = 'null'

        flight_id_1 = str(temp_1['flight_id'])
        flight_id_2 = str(temp_2['flight_id'])

        postgres_sql_text = f"INSERT INTO \"{table_name}\" " \
                             "(\"flight_id\"," + \
                             "\"aircraft_id\"," \
                             "\"start_time_unix\"," \
                             "\"callsign\"," \
                             "\"reg\"," \
                             "\"adep_iata\"," \
                             "\"ades_iata\"," \
                             "\"actype\"," \
                             "\"geom\"," \
                             "\"end_time_unix\")"

        postgres_sql_text = postgres_sql_text + " VALUES('" + flight_id_1 + "','" \
                            + aircraft_id + "'," \
                            + snapshot_id_1 + ",'" \
                            + callsign + "','" \
                            + reg + "','" \
                            + schd_from + "','" \
                            + schd_to + "','" \
                            + equip + "'," \
                            + "ST_LineFromText('LINESTRING("

        latitude_1 = str(temp_1['latitude'])
        longitude_1 = str(temp_1['longitude'])

        latitude_2 = str(temp_2['latitude'])
        longitude_2 = str(temp_2['longitude'])

        snapshot_id_1 = str(temp_1['snapshot_id'])
        snapshot_id_2 = str(temp_2['snapshot_id'])

        altitude_1 = str(temp_1['altitude'])
        altitude_2 = str(temp_2['altitude'])

        while k < num_of_records - 1:
            while (temp_1['flight_id'] == temp_2['flight_id']) and \
                 (temp_2['snapshot_id'] - temp_1['snapshot_id']) < 300:
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

                latitude_1 = str(temp_1['latitude'])

                longitude_1 = str(temp_1['longitude'])

                altitude_1 = str(temp_1['altitude'])

                snapshot_id_1 = str(temp_1['snapshot_id'])

                #----------------
                aircraft_id = str(temp_1['aircraft_id'])
                if aircraft_id == 'None':
                    aircraft_id = 'null'

                reg = str(temp_1['reg'])
                if reg == 'None':
                    reg = 'null'

                schd_from = str(temp_1['schd_from'])
                if schd_from == 'None':
                    schd_from = 'null'

                schd_to = str(temp_1['schd_to'])
                if schd_to == 'None':
                    schd_to = 'null'

                equip = str(temp_1['equip'])
                if equip == 'None':
                    equip = 'null'

                callsign = str(temp_1['callsign'])
                if callsign == 'None':
                    callsign = 'null'
                # ----------------

                temp_2 = record[k + 1]

            postgres_sql_text = postgres_sql_text + \
                                        longitude_1 + " " + latitude_1 + " " + \
                                        altitude_1 + ","

            postgres_sql_text = postgres_sql_text + \
                                        longitude_1 + " " + latitude_1 + " " + \
                                        altitude_1 + ")',4326),"

            postgres_sql_text += snapshot_id_1 + ")"

            print(postgres_sql_text)
            cursor_postgres.execute(postgres_sql_text)
            conn_postgres.commit()

            postgres_sql_text = "INSERT INTO \"" + table_name + "\" " \
                                "(\"flight_id\"," + \
                                "\"aircraft_id\"," \
                                "\"start_time_unix\"," \
                                "\"callsign\"," \
                                "\"reg\"," \
                                "\"adep_iata\"," \
                                "\"ades_iata\"," \
                                "\"actype\"," \
                                "\"geom\"," \
                                "\"end_time_unix\") "

            postgres_sql_text = postgres_sql_text + " VALUES('" + flight_id_1 + "','" \
                                + aircraft_id + "'," \
                                + snapshot_id_1 + ",'" \
                                + callsign + "','" \
                                + reg + "','" \
                                + schd_from + "','" \
                                + schd_to + "','" \
                                + equip + "'," \
                                + "ST_LineFromText('LINESTRING("

            #print(postgres_sql_text)

            latitude_1 = str(temp_1['latitude'])
            longitude_1 = str(temp_1['longitude'])

            latitude_2 = str(temp_2['latitude'])
            longitude_2 = str(temp_2['longitude'])

            snapshot_id_1 = str(temp_1['snapshot_id'])
            snapshot_id_2 = str(temp_2['snapshot_id'])

            altitude_1 = str(temp_1['altitude'])
            altitude_2 = str(temp_2['altitude'])

            #cursor_postgres.execute(postgres_sql_text)

            #conn_postgres.commit()

            print(str("{:.3f}".format((k / num_of_records) * 100,2)) + "% Completed")
            #print(str("{:.3f}".format((k / num_of_records) * 100,2)) + "% Completed")

            if num_of_records-k <= 2 :

                postgres_sql_text = "DROP TABLE IF EXISTS " + table_name + "_icao_code; " + \
                                    "SELECT t.*, " + \
                                    "a1.airport_identifier as dep , " + \
                                    "a2.airport_identifier as dest, " + \
                                    "to_timestamp(start_time_unix::INT - 7*60*60)::timestamp without time zone at time zone 'Etc/UTC' as start_time, " + \
                                    "to_timestamp(end_time_unix::INT - 7*60*60)::timestamp without time zone at time zone 'Etc/UTC' as end_time " + \
                                    f"INTO track_{year_month_day}_fr24_icao_code " + \
                                    f"FROM public.track_{year_month_day}_fr24 t " + \
                                    "LEFT JOIN airports a1 " + \
                                    "ON a1.iata_ata_designator = t.adep_iata " + \
                                    "LEFT JOIN airports a2 " + \
                                    "ON a2.iata_ata_designator = t.ades_iata;" + \
                                    f"DROP TABLE IF EXISTS track_{year_month_day}_fr24; " + \
                                    f"ALTER TABLE track_{year_month_day}_fr24_icao_code RENAME TO track_{year_month_day}_fr24;"
                print(postgres_sql_text)
                cursor_postgres.execute(postgres_sql_text)
                conn_postgres.commit()
                break

            k = k + 1
            if num_of_records - k <= 2:
                break
            temp_1 = record[k]
            temp_2 = record[k + 1]

            latitude_1 = str(temp_1['latitude'])
            longitude_1 = str(temp_1['longitude'])

            latitude_2 = str(temp_2['latitude'])
            longitude_2 = str(temp_2['longitude'])

            snapshot_id_1 = str(temp_1['snapshot_id'])
            snapshot_id_2 = str(temp_2['snapshot_id'])

            altitude_1 = str(temp_1['altitude'])
            altitude_2 = str(temp_2['altitude'])

            flight_id_1 = str(temp_1['flight_id'])
            flight_id_2 = str(temp_2['flight_id'])

            if k < num_of_records:

                postgres_sql_text = "INSERT INTO \"" + table_name + "\" " \
                                     "(\"flight_id\"," + \
                                    "\"aircraft_id\"," \
                                    "\"start_time_unix\"," \
                                    "\"callsign\"," \
                                    "\"reg\"," \
                                    "\"adep_iata\"," \
                                    "\"ades_iata\"," \
                                    "\"actype\"," \
                                    "\"geom\"," \
                                    "\"end_time_unix\")"

                # ----------------
                aircraft_id = str(temp_1['aircraft_id'])
                if aircraft_id == 'None':
                    aircraft_id = 'null'

                reg = str(temp_1['reg'])
                if reg == 'None':
                    reg = 'null'

                schd_from = str(temp_1['schd_from'])
                if schd_from == 'None':
                    schd_from = 'null'

                schd_to = str(temp_1['schd_to'])
                if schd_to == 'None':
                    schd_to = 'null'

                equip = str(temp_1['equip'])
                if equip == 'None':
                    equip = 'null'

                callsign = str(temp_1['callsign'])
                if callsign == 'None':
                    callsign = 'null'

                snapshot_id_1 = str(temp_1['snapshot_id'])
                flight_id_1 = str(temp_1['flight_id'])
                # ----------------

                postgres_sql_text = postgres_sql_text + " VALUES('" + flight_id_1 + "','" \
                                    + aircraft_id + "'," \
                                    + snapshot_id_1 + ",'" \
                                    + callsign + "','" \
                                    + reg + "','" \
                                    + schd_from + "','" \
                                    + schd_to + "','" \
                                    + equip + "'," \
                                    + "ST_LineFromText('LINESTRING("

            else:
                break