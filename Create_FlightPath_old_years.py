import psycopg2
import datetime
import mysql.connector
import math
import time

from mysql.connector import Error

t = time.time()

# Try to connect to the local PostGresSQL database in which we will store our flight trajectories coupled with FPL data.
try:
    conn_postgres = psycopg2.connect(user = "postgres",
                                  password = "password",
                                  host = "127.0.0.1",
                                  port = "5432",
                                  database = "flight_track")

    cursor_postgres = conn_postgres.cursor()

# create the table name that will store the radar track
    year_list = ['2015','2016','2017']
    month_list = ['01','02','03','04','05','06','07','08','09','10','11','12']

    for year in year_list:
        for month in month_list:

            year_month = year + '_' + month

            table_name = "track_" + year_month + ""

            center_map_latitude = 13.89
            center_map_longitude = 100.6
            lat_offset_scale = 0
            lon_offset_scale = 0

            Re = 6371  # Earth Radius in km

            # radar target positions (lat, long) adjustment parameters
            # center_map_latitude = 13
            # center_map_longitude = 101.3
            # lat_offset_scale = .21
            # lon_offset_scale = .3

            # Create an sql query that creates a new table for radar tracks in Postgres SQL database
            postgres_sql_text = "DROP TABLE IF EXISTS " + table_name + "; \n" + \
                                "CREATE TABLE " + table_name + " " + \
                                "(callsign character varying, flight_id integer, geom geometry, " + \
                                "start_time timestamp without time zone, " + \
                                "etd timestamp without time zone, " + \
                                "atd timestamp without time zone, " + \
                                "eta timestamp without time zone, " + \
                                "ata timestamp without time zone, " + \
                                "end_time timestamp without time zone, " + \
                                "dep character varying, dest character varying, " + \
                                "actype character varying, " + \
                                "runway character varying, " + \
                                "sidstar character varying, " + \
                                "op_type character varying, " + \
                                "frule character varying, " + \
                                "reg character varying, " + \
                                "pbn_type character varying, " + \
                                "entry_flevel integer, " + \
                                "maintain_flevel integer, " + \
                                "exit_flevel integer, " + \
                                "comnav character varying, flevel integer,route character varying)" + \
                                "WITH (OIDS=FALSE); \n" + \
                                "ALTER TABLE " + table_name + " " \
                                "OWNER TO postgres;"
            print(postgres_sql_text)
            cursor_postgres.execute(postgres_sql_text)

            conn_postgres.commit()

            # Try to connect to the remote MySQL database which contains flight plan data and radar target data.
            try:
                conn_mysql = mysql.connector.connect(host='172.16.101.32',
                                                     database='flight',
                                                     user='pabhakara',
                                                     password='327146ra',
                                                     auth_plugin='mysql_native_password')


                if conn_mysql.is_connected():

                    # Select flight plan data and radar target data from MySQL database
                    mysql_query = "SELECT a.flight_id,b.app_time,a.dep,a.dest,a.actype,a.ftype,a.op_type,a.etd,a.atd,a.eta,a.ata,a.flevel," + \
                    "b.latitude,b.longitude,b.actual_flight_level,b.cdm," + \
                    "b.sector,a.callsign,a.route,a.pbn_type,a.comnav," + \
                    "a.sidstar,a.runway,a.frule,a.reg,a.entry_flevel,a.maintain_flevel,a.exit_flevel " + \
                    "from " + year_month + "_radar a, target_" + year_month + " b " + \
                    "where (a.flight_id = b.flight_id) " + \
                    "and (day(a.entry_time) < 32) and (day(a.entry_time) >=1 ) " + \
                    "order by a.flight_id,b.app_time"
                    #print(mysql_query)

                    cursor_mysql = conn_mysql.cursor(dictionary=True)
                    cursor_mysql.execute(mysql_query)
                    record = cursor_mysql.fetchall()
                    num_of_records = len(record)
                    print("num_of_record: ",num_of_records)

                    k = 0

                    temp_1 = record[k]
                    temp_2 = record[k+1]

                    callsign = str(temp_1['callsign'])
                    app_time = str(temp_1['app_time'])

                    etd = str(temp_1['etd'])
                    if etd == 'None':
                        etd = 'null'
                    else:
                        etd = "'" + etd + "'"
                    atd = str(temp_1['atd'])
                    if atd == 'None':
                        atd = 'null'
                    else:
                        atd = "'" + atd + "'"
                    eta = str(temp_1['eta'])
                    if eta == 'None':
                        eta = 'null'
                    else:
                        eta = "'" + eta + "'"
                    ata = str(temp_1['ata'])
                    if ata == 'None':
                        ata = 'null'
                    else:
                        ata = "'" + ata + "'"

                    dep = str(temp_1['dep'])
                    dest = str(temp_1['dest'])
                    reg = str(temp_1['reg'])

                    actype = str(temp_1['actype'])
                    runway = str(temp_1['runway'])
                    sidstar = str(temp_1['sidstar'])
                    pbn_type = str(temp_1['pbn_type'])
                    op_type = str(temp_1['op_type'])
                    frule = str(temp_1['frule'])
                    comnav = str(temp_1['comnav'])
                    route = str(temp_1['route'])

                    flevel = str(temp_1['flevel'])
                    if flevel == 'None':
                        #print("flevel = None")
                        flevel = "-1"

                    entry_flevel = str(temp_1['entry_flevel'])
                    if entry_flevel == 'None':
                        #print("entry_flevel = None")
                        entry_flevel = "-1"

                    maintain_flevel = str(temp_1['maintain_flevel'])
                    if maintain_flevel == 'None':
                        #print("maintain_flevel = None")
                        maintain_flevel = "-1"

                    exit_flevel = str(temp_1['exit_flevel'])
                    if exit_flevel == 'None':
                        #print("exit_flevel = None")
                        exit_flevel = "-1"

                    flight_id_1 = str(temp_1['flight_id'])
                    flight_id_2 = str(temp_2['flight_id'])

                    postgres_sql_text = "INSERT INTO \"" + table_name + "\" (\"callsign\"," + \
                                        "\"flight_id\",\"start_time\",\"etd\",\"atd\",\"eta\",\"ata\"," + \
                                        "\"dep\",\"dest\",\"reg\",\"actype\",\"runway\"," + \
                                        "\"sidstar\",\"pbn_type\"," + \
                                        "\"op_type\",\"frule\",\"comnav\",\"route\"," + \
                                        "\"flevel\",\"entry_flevel\",\"maintain_flevel\",\"exit_flevel\"," + \
                                        "\"geom\",\"end_time\")"

                    postgres_sql_text = postgres_sql_text + " VALUES('" + callsign + "'," \
                            + flight_id_1 + ",'" \
                            + app_time + "'," \
                            + etd + "," \
                            + atd + "," \
                            + eta + "," \
                            + ata + ",'" \
                            + dep + "','" \
                            + dest + "','" \
                            + reg + "','" \
                            + actype + "','" \
                            + runway + "','" \
                            + sidstar + "','" \
                            + pbn_type + "','" \
                            + op_type + "','" \
                            + frule + "','" \
                            + comnav + "','" \
                            + route + "','" \
                            + flevel + "','" \
                            + entry_flevel + "','" \
                            + maintain_flevel + "','" \
                            + exit_flevel + "'," \
                            + "ST_LineFromText('LINESTRING("

                    offset_1 = (Re ** 2 + ((center_map_latitude - float(temp_1['latitude'])) * 60 * 1.852) ** 2) ** 0.5 - Re
                    offset_lat_1 = offset_1 * 1.852 / 60. * math.sin(math.radians((float(temp_1['latitude'])) - center_map_latitude)) * lat_offset_scale
                    latitude_1 = str(float(temp_1['latitude']) + offset_lat_1)

                    offset_2 = (Re ** 2 + ((center_map_latitude - float(temp_2['latitude'])) * 60 * 1.852) ** 2) ** 0.5 - Re
                    offset_lat_2 = offset_2 * 1.852 / 60. * math.sin(math.radians((float(temp_2['latitude'])) - center_map_latitude)) * lat_offset_scale
                    latitude_2 = str(float(temp_2['latitude']) + offset_lat_2)

                    offset_3 = (Re ** 2 + ((center_map_longitude - float(temp_1['longitude'])) * 60 * 1.852) ** 2) ** 0.5 - Re
                    offset_lon_1 = offset_3 * 1.852 / 60. * math.sin(math.radians((float(temp_1['longitude'])) - center_map_longitude)) * lon_offset_scale
                    longitude_1 =  str(float(temp_1['longitude']) + offset_lon_1)

                    offset_4 = (Re ** 2 + ((center_map_longitude - float(temp_2['longitude'])) * 60 * 1.852) ** 2) ** 0.5 - Re
                    offset_lon_2 = offset_4 * 1.852 / 60. * math.sin(math.radians((float(temp_2['longitude'])) - center_map_longitude)) * lon_offset_scale
                    longitude_2 = str(float(temp_1['longitude']) + offset_lon_2)

                    app_time_1 = str(temp_1['app_time'])

                    actual_flight_level_1 = str(temp_1['actual_flight_level'])

                    app_time_2 = str(temp_2['app_time'])

                    actual_flight_level_2 = str(temp_2['actual_flight_level'])

                    while k < num_of_records - 1:
                        while (temp_1['flight_id'] == temp_2['flight_id']) and \
                            abs(temp_2['longitude'] - temp_1['longitude']) < 1 and \
                            abs(temp_2['latitude'] - temp_1['latitude']) < 1:
                            #(temp_2['app_time'] - temp_1['app_time']) <= datetime.timedelta(minutes=1) and \
                            #abs(temp_2['longitude'] - temp_1['longitude']) < 1 and \
                            #abs(temp_2['latitude'] - temp_1['latitude']) < 1:

                            postgres_sql_text = postgres_sql_text + \
                                                longitude_1 + " " + latitude_1 + " " + \
                                                actual_flight_level_1 + ","
                            k = k + 1
                            if k == num_of_records-1:
                                break
                            temp_1 = record[k]

                            offset_1 = (Re ** 2 + ((center_map_latitude - float(temp_1['latitude'])) * 60 * 1.852) ** 2) ** 0.5 - Re
                            offset_lat_1 = offset_1 * 1.852 / 60. * math.sin(math.radians((float(temp_1['latitude'])) - center_map_latitude)) \
                                           * lat_offset_scale
                            latitude_1 = str(float(temp_1['latitude']) + offset_lat_1)

                            offset_3 = (Re ** 2 + ((center_map_longitude - float(temp_1['longitude'])) * 60 * 1.852) ** 2) ** 0.5 - Re
                            offset_lon_1 = offset_3 * 1.852 / 60. * math.sin(math.radians((float(temp_1['longitude'])) - center_map_longitude)) \
                                           * lon_offset_scale
                            longitude_1 = str(float(temp_1['longitude']) + offset_lon_1)

                            app_time_1 = str(temp_1['app_time'])

                            temp_2 = record[k + 1]

                        postgres_sql_text = postgres_sql_text + \
                                                longitude_1 + " " + latitude_1 + " " + \
                                                actual_flight_level_1 + ","

                        postgres_sql_text = postgres_sql_text + \
                                            longitude_1 + " " + latitude_1 + " " + \
                                            actual_flight_level_1 + ")',4326),'"

                        postgres_sql_text = postgres_sql_text + \
                                            app_time_1 +"')"

                        #print(postgres_sql_text)
                        cursor_postgres.execute(postgres_sql_text)
                        conn_postgres.commit()
                        print(str("{:.3f}".format((k / num_of_records) * 100,2)) + "% Completed")

                        if num_of_records - k <= 2:
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

                        #-----

                        offset_1 = (Re ** 2 + ((center_map_latitude - float(temp_1['latitude'])) * 60 * 1.852) ** 2) ** 0.5 - Re
                        offset_lat_1 = offset_1 * 1.852 / 60. * math.sin(math.radians((float(temp_1['latitude'])) - center_map_latitude)) \
                                       * lat_offset_scale
                        latitude_1 = str(float(temp_1['latitude']) + offset_lat_1)

                        offset_2 = (Re ** 2 + ((center_map_latitude - float(temp_2['latitude'])) * 60 * 1.852) ** 2) ** 0.5 - Re
                        offset_lat_2 = offset_2 * 1.852 / 60. * math.sin(math.radians((float(temp_2['latitude'])) - center_map_latitude)) \
                                       * lat_offset_scale
                        latitude_2 = str(float(temp_2['latitude']) + offset_lat_2)

                        offset_3 = (Re ** 2 + ((center_map_longitude - float(temp_1['longitude'])) * 60 * 1.852) ** 2) ** 0.5 - Re
                        offset_lon_1 = offset_3 * 1.852 / 60. * math.sin(math.radians((float(temp_1['longitude'])) - center_map_longitude)) \
                                       * lon_offset_scale
                        longitude_1 = str(float(temp_1['longitude']) + offset_lon_1)

                        offset_4 = (Re ** 2 + ((center_map_longitude - float(temp_2['longitude'])) * 60 * 1.852) ** 2) ** 0.5 - Re
                        offset_lon_2 = offset_4 * 1.852 / 60. * math.sin(math.radians((float(temp_2['longitude'])) - center_map_longitude)) \
                                       * lon_offset_scale
                        longitude_2 = str(float(temp_1['longitude']) + offset_lon_2)

                        app_time_1 = str(temp_1['app_time'])

                        actual_flight_level_1 = str(temp_1['actual_flight_level'])

                        app_time_2 = str(temp_2['app_time'])

                        actual_flight_level_2 = str(temp_2['actual_flight_level'])

                        #-----

                        if k < num_of_records:

                            postgres_sql_text = "INSERT INTO \"" + table_name + "\" (\"callsign\"," + \
                                                "\"flight_id\",\"start_time\",\"etd\",\"atd\",\"eta\",\"ata\"," + \
                                                "\"dep\",\"dest\",\"reg\",\"actype\",\"runway\"," + \
                                                "\"sidstar\",\"pbn_type\"," + \
                                                "\"op_type\",\"frule\",\"comnav\",\"route\"," + \
                                                "\"flevel\",\"entry_flevel\",\"maintain_flevel\",\"exit_flevel\"," + \
                                                "\"geom\",\"end_time\")"

                            callsign = str(temp_1['callsign'])
                            app_time = str(temp_1['app_time'])

                            etd = str(temp_1['etd'])
                            if etd == 'None':
                                etd = 'null'
                            else:
                                etd = "'" + etd + "'"
                            atd = str(temp_1['atd'])
                            if atd == 'None':
                                atd = 'null'
                            else:
                                atd = "'" + atd + "'"
                            eta = str(temp_1['eta'])
                            if eta == 'None':
                                eta = 'null'
                            else:
                                eta = "'" + eta + "'"
                            ata = str(temp_1['ata'])
                            if ata == 'None':
                                ata = 'null'
                            else:
                                ata = "'" + ata + "'"

                            dep = str(temp_1['dep'])
                            dest = str(temp_1['dest'])
                            reg = str(temp_1['reg'])

                            actype = str(temp_1['actype'])
                            runway = str(temp_1['runway'])
                            sidstar = str(temp_1['sidstar'])
                            pbn_type = str(temp_1['pbn_type'])
                            op_type = str(temp_1['op_type'])
                            frule = str(temp_1['frule'])
                            comnav = str(temp_1['comnav'])
                            route = str(temp_1['route'])

                            flevel = str(temp_1['flevel'])

                            # Check if flevel in FPL is not empty.
                            if flevel == 'None':
                                #print("flevel = None")
                                flevel = "-1"

                            entry_flevel = str(temp_1['entry_flevel'])
                            if entry_flevel == 'None':
                                #print("entry_flevel = None")
                                entry_flevel = "-1"

                            maintain_flevel = str(temp_1['maintain_flevel'])
                            if maintain_flevel == 'None':
                                #print("maintain_flevel = None")
                                maintain_flevel = "-1"

                            exit_flevel = str(temp_1['exit_flevel'])
                            if exit_flevel == 'None':
                                #print("exit_flevel = None")
                                exit_flevel = "-1"

                            flight_id_1 = str(temp_1['flight_id'])

                            postgres_sql_text = postgres_sql_text + " VALUES('" + callsign + "'," \
                            + flight_id_1 + ",'" \
                            + app_time + "'," \
                            + etd + "," \
                            + atd + "," \
                            + eta + "," \
                            + ata + ",'" \
                            + dep + "','" \
                            + dest + "','" \
                            + reg + "','" \
                            + actype + "','" \
                            + runway + "','" \
                            + sidstar + "','" \
                            + pbn_type + "','" \
                            + op_type + "','" \
                            + frule + "','" \
                            + comnav + "','" \
                            + route + "','" \
                            + flevel + "','" \
                            + entry_flevel + "','" \
                            + maintain_flevel + "','" \
                            + exit_flevel + "'," \
                            + "ST_LineFromText('LINESTRING("
                        else:
                            break
                    cursor_postgres.execute(postgres_sql_text)
                    conn_postgres.commit()

            except Error as e:
                print("Error while connecting to MySQL", e)
                elapsed = time.time() - t
                print('Elapsed: %s sec' % elapsed)
            finally:
                if conn_mysql.is_connected():
                    cursor_mysql.close()
                    conn_mysql.close()
                    print("MySQL connection is closed")
                    elapsed = time.time() - t
                    print('Elapsed: %s sec' % elapsed)

except (Exception, psycopg2.Error) as error :
    print ("Error while connecting to PostgreSQL", error)
finally:
    #closing database connection.
        if conn_postgres:
            cursor_postgres.close()
            conn_postgres.close()
            print("PostgreSQL connection is closed")
            elapsed = time.time() - t
            print('Elapsed: %s sec' % elapsed)