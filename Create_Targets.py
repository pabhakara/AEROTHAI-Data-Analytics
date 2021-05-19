import psycopg2
import datetime
import mysql.connector
import math

from mysql.connector import Error

# Try to connect to the local PostGresSQL database in which we will store our flight trajectories coupled with FPL data.
try:
    conn_postgres = psycopg2.connect(user = "postgres",
                                  password = "password",
                                  host = "127.0.0.1",
                                  port = "5432",
                                  database = "postgres")

    cursor_postgres = conn_postgres.cursor()


    # create the table name that will store the radar track
    year_month = "2019_05"
    table_name = "radar_targets_" + year_month + "_x"

    # center_map_latitude = 13.89
    # center_map_longitude = 100.6

    Re = 6371  # Earth Radius in km

    # radar target positions (lat, long) adjustment parameters
    center_map_latitude = 13
    center_map_longitude = 101.3
    lat_offset_scale = .21
    lon_offset_scale = .3

    # Create an sql query that creates a new table for radar tracks in Postgres SQL database
    postgres_sql_text = "DROP TABLE IF EXISTS " + table_name + "; \n" + \
                        "CREATE TABLE " + table_name + " " + \
                        "(callsign character varying, flight_id integer, geom geometry, " + \
                        "app_time timestamp without time zone, " + \
                        "dep character varying, dest character varying, " + \
                        "actype character varying, " + \
                        "op_type character varying, " + \
                        "frule character varying, " + \
                        "reg character varying, " + \
                        "pbn_type character varying, " + \
                        "comnav character varying, flevel integer)" + \
                        "WITH (OIDS=FALSE); \n" + \
                        "ALTER TABLE " + table_name + " " \
                        "OWNER TO postgres;"
    print(postgres_sql_text)
    cursor_postgres.execute(postgres_sql_text)

    conn_postgres.commit()

    # Try to connect to the local MySQL database which contains flight plan data and radar target data.
    try:
        conn_mysql = mysql.connector.connect(host='localhost',
                                             database='flight',
                                             user='root',
                                             password='password')
        if conn_mysql.is_connected():

            # Select flight plan data and radar target data from MySQL database
            mysql_query = "SELECT b.app_time,b.callsign" + \
            "b.latitude,b.longitude,b.actual_flight_level,b.cdm," + \
            "b.sector " + \
            "from " + year_month + "_radar a, target_" + year_month + " b " + \
            "order by b.app_time"
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
                print("flevel = None")
                flevel = "0"

            entry_flevel = str(temp_1['entry_flevel'])
            maintain_flevel = str(temp_1['maintain_flevel'])
            exit_flevel = str(temp_1['exit_flevel'])

            flight_id_1 = str(temp_1['flight_id'])
            flight_id_2 = str(temp_2['flight_id'])

            postgres_sql_text = "INSERT INTO \"" + table_name + "\" (\"callsign\"," + \
                                "\"actual_flight_level\",\"cdm\",\"app_time\",
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
                        print("flevel = None")
                        flevel = "0"

                    entry_flevel = str(temp_1['entry_flevel'])
                    maintain_flevel = str(temp_1['maintain_flevel'])
                    exit_flevel = str(temp_1['exit_flevel'])

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
            #cursor_postgres.execute(postgres_sql_text)
            #conn_postgres.commit()

    except Error as e:
        print("Error while connecting to MySQL", e)
    finally:
        if conn_mysql.is_connected():
            cursor_mysql.close()
            conn_mysql.close()
            print("MySQL connection is closed")

except (Exception, psycopg2.Error) as error :
    print ("Error while connecting to PostgreSQL", error)
finally:
    #closing database connection.
        if conn_postgres:
            cursor_postgres.close()
            conn_postgres.close()
            print("PostgreSQL connection is closed")