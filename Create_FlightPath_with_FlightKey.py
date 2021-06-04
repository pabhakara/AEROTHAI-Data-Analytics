import psycopg2
import psycopg2.extras
import datetime
import mysql.connector
import math
import time

from mysql.connector import Error
# Need to connect to AEROTHAI's MySQL Server


# Try to connect to the local PostGresSQL database in which we will store our flight trajectories coupled with FPL data.
conn_postgres = psycopg2.connect(user = "postgres",
                                 password = "password",
                                 host = "127.0.0.1",
                                 port = "5432",
                                 database = "test_db")
with conn_postgres:

    cursor_postgres = conn_postgres.cursor(cursor_factory = psycopg2.extras.DictCursor)

    year = '2021'
    month = '02'
    day_list = range(3,29)

    t = time.time()

    year_month = str(year) + '_' + str(month)

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


    # postgres_sql_text = "DROP TABLE IF EXISTS " + table_name + "_flightkey; \n" + \
    #                     "CREATE TABLE " + table_name + "_flightkey " + \
    #                     "(callsign character varying, flight_id integer, " \
    #                     "flight_key character varying, geom geometry, " + \
    #                     "start_time timestamp without time zone, " + \
    #                     "etd timestamp without time zone, " + \
    #                     "atd timestamp without time zone, " + \
    #                     "eta timestamp without time zone, " + \
    #                     "ata timestamp without time zone, " + \
    #                     "end_time timestamp without time zone, " + \
    #                     "dep character varying, dest character varying, " + \
    #                     "actype character varying, " + \
    #                     "frule character varying, " + \
    #                     "reg character varying, " + \
    #                     "item_18 character varying, " + \
    #                     "comnav character varying, flevel character varying, " + \
    #                     "processedfixroute character varying," + \
    #                     "processedfix character varying," + \
    #                     "processedroute character varying)" + \
    #                     "WITH (OIDS=FALSE); \n" + \
    #                     "ALTER TABLE " + table_name + "_flightkey " \
    #                                                   "OWNER TO postgres;"
    # print(postgres_sql_text)
    # cursor_postgres.execute(postgres_sql_text)
    #
    # conn_postgres.commit()



    for day in day_list:
        # Try to connect to the remote MySQL database which contains flight plan data and radar target data.
        # try:
        #     conn_mysql = mysql.connector.connect(host='172.16.101.32',
        #                                          database='flight',
        #                                          user='pabhakara',
        #                                          password='327146ra',
        #                                          auth_plugin='mysql_native_password')

        # if conn_mysql.is_connected():
        # Select flight plan data and radar target data from MySQL database
        postgres_sql_text = "SELECT a.\"FlightID\",a.\"FlightKey\",b.\"APPTIME\",a.\"DEP\",a.\"DEST\",a.\"AcType\",a.\"ETD\",a.\"ATD\",a.\"ETA\",a.\"ATA\",a.\"FLEVEL\"," + \
                            "b.\"LATITUDE\",b.\"LONGITUDE\",b.\"AFLEVEL\"," + \
                            "a.\"C/S\",a.\"Item18\",a.\"COMNAV\"," + \
                            "a.\"ProcessedFixRoute\",a.\"ProcessedFix\", a.\"ProcessedRoute\"," + \
                            "a.\"FRULE\",a.\"REG\" " + \
                            "from fdms_flight_plan_processed a, target_" + year_month + "_5s  b " + \
                            "where (a.\"FlightKey\"= b.\"FlightKey\") " + \
                            "and (extract(day from (a.\"DOF\")) < " + str(day+1) +") and (extract(day from (a.\"DOF\")) >= + "+str(day)+" ) " + \
                            "order by a.\"FlightKey\",b.\"APPTIME\""
        #print(mysql_query)

        print(postgres_sql_text)
        cursor_postgres.execute(postgres_sql_text)

        record = cursor_postgres.fetchall()

        num_of_records = len(record)
        print("num_of_record: ", num_of_records)

        k = 0

        temp_1 = record[k]
        temp_2 = record[k+1]

        callsign = str(temp_1['C/S'])
        app_time = str(temp_1['APPTIME'])

        etd = str(temp_1['ETD'])
        if etd == 'None':
            etd = 'null'
        else:
            etd = "'" + etd + "'"
        atd = str(temp_1['ATD'])
        if atd == 'None':
            atd = 'null'
        else:
            atd = "'" + atd + "'"
        eta = str(temp_1['ETA'])
        if eta == 'None':
            eta = 'null'
        else:
            eta = "'" + eta + "'"
        ata = str(temp_1['ATA'])
        if ata == 'None':
            ata = 'null'
        else:
            ata = "'" + ata + "'"

        dep = str(temp_1['DEP'])
        dest = str(temp_1['DEST'])
        reg = str(temp_1['REG'])
        actype = str(temp_1['AcType'])
        item_18 = str(temp_1['Item18'])
        frule = str(temp_1['FRULE'])
        comnav = str(temp_1['COMNAV'])
        processedfixroute = str(temp_1['ProcessedFixRoute'])
        processedfix = str(temp_1['ProcessedFix'])
        processedroute = str(temp_1['ProcessedRoute'])

        flevel = str(temp_1['FLEVEL'])
        if flevel == 'None':
            # print("flevel = None")
            flevel = "-1"

        flight_id_1 = str(temp_1['FlightID'])
        flight_id_2 = str(temp_2['FlightID'])

        flight_key_1 = str(temp_1['FlightKey'])
        flight_key_2 = str(temp_2['FlightKey'])

        postgres_sql_text = "INSERT INTO \"" + table_name + "_flightkey" + "\" (\"callsign\"," + \
                            "\"flight_id\",\"flight_key\",\"start_time\",\"etd\",\"atd\",\"eta\",\"ata\"," + \
                            "\"dep\",\"dest\",\"reg\",\"actype\"," + \
                            "\"item_18\"," + \
                            "\"frule\",\"comnav\","+ \
                            "\"processedfixroute\",\"processedfix\",\"processedroute\"," + \
                            "\"flevel\"," + \
                            "\"geom\",\"end_time\")"

        postgres_sql_text = postgres_sql_text + " VALUES('" + callsign + "'," \
                            + flight_id_1 + ",'" \
                            + flight_key_1 + "','" \
                            + app_time + "'," \
                            + etd + "," \
                            + atd + "," \
                            + eta + "," \
                            + ata + ",'" \
                            + dep + "','" \
                            + dest + "','" \
                            + reg + "','" \
                            + actype + "','" \
                            + item_18 + "','" \
                            + frule + "','" \
                            + comnav + "','" \
                            + processedfixroute + "','" \
                            + processedfix + "','" \
                            + processedroute + "','" \
                            + flevel + "'," \
                            + "ST_LineFromText('LINESTRING("

        latitude_1 = str(float(temp_1['LATITUDE']))
        latitude_2 = str(float(temp_2['LATITUDE']))

        longitude_1 =  str(float(temp_1['LONGITUDE']))
        longitude_2 = str(float(temp_2['LONGITUDE']))

        app_time_1 = str(temp_1['APPTIME'])

        actual_flight_level_1 = str(temp_1['AFLEVEL'])

        app_time_2 = str(temp_2['APPTIME'])

        actual_flight_level_2 = str(temp_2['AFLEVEL'])

        while k < num_of_records - 1:
            while (temp_1['FlightKey'] == temp_2['FlightKey']) and \
                    abs(temp_2['LONGITUDE'] - temp_1['LONGITUDE']) < 1 and \
                    abs(temp_2['LATITUDE'] - temp_1['LATITUDE']) < 1:
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

                latitude_1 = str(float(temp_1['LATITUDE']))
                longitude_1 = str(float(temp_1['LONGITUDE']))

                app_time_1 = str(temp_1['APPTIME'])

                temp_2 = record[k + 1]

            postgres_sql_text = postgres_sql_text + \
                                longitude_1 + " " + latitude_1 + " " + \
                                actual_flight_level_1 + ","

            postgres_sql_text = postgres_sql_text + \
                                longitude_1 + " " + latitude_1 + " " + \
                                actual_flight_level_1 + ")',4326),'"

            postgres_sql_text = postgres_sql_text + \
                                app_time_1 +"')"

            print(postgres_sql_text)
            cursor_postgres.execute(postgres_sql_text)
            conn_postgres.commit()
            print(str("{:.3f}".format((k / num_of_records) * 100,2)) + "% Completed")

            if num_of_records - k <= 2:
                break

            k = k + 1
            temp_1 = record[k]
            temp_2 = record[k + 1]

            #-----

            # offset_1 = (Re ** 2 + ((center_map_latitude - float(temp_1['latitude'])) * 60 * 1.852) ** 2) ** 0.5 - Re
            # offset_lat_1 = offset_1 * 1.852 / 60. * math.sin(math.radians((float(temp_1['latitude'])) - center_map_latitude)) \
            #                * lat_offset_scale
            latitude_1 = str(float(temp_1['LATITUDE']))
            #
            # offset_2 = (Re ** 2 + ((center_map_latitude - float(temp_2['latitude'])) * 60 * 1.852) ** 2) ** 0.5 - Re
            # offset_lat_2 = offset_2 * 1.852 / 60. * math.sin(math.radians((float(temp_2['latitude'])) - center_map_latitude)) \
            #                * lat_offset_scale
            latitude_2 = str(float(temp_2['LATITUDE']))
            #
            # offset_3 = (Re ** 2 + ((center_map_longitude - float(temp_1['longitude'])) * 60 * 1.852) ** 2) ** 0.5 - Re
            # offset_lon_1 = offset_3 * 1.852 / 60. * math.sin(math.radians((float(temp_1['longitude'])) - center_map_longitude)) \
            #                * lon_offset_scale
            longitude_1 = str(float(temp_1['LONGITUDE']))
            #
            # offset_4 = (Re ** 2 + ((center_map_longitude - float(temp_2['longitude'])) * 60 * 1.852) ** 2) ** 0.5 - Re
            # offset_lon_2 = offset_4 * 1.852 / 60. * math.sin(math.radians((float(temp_2['longitude'])) - center_map_longitude)) \
            #                * lon_offset_scale
            longitude_2 = str(float(temp_2['LONGITUDE']))

            app_time_1 = str(temp_1['APPTIME'])

            actual_flight_level_1 = str(temp_1['AFLEVEL'])

            app_time_2 = str(temp_2['APPTIME'])

            actual_flight_level_2 = str(temp_2['AFLEVEL'])

            if k < num_of_records:

                postgres_sql_text = "INSERT INTO \"" + table_name + "_flightkey" + "\" (\"callsign\"," + \
                                    "\"flight_id\",\"flight_key\",\"start_time\",\"etd\",\"atd\",\"eta\",\"ata\"," + \
                                    "\"dep\",\"dest\",\"reg\",\"actype\"," + \
                                    "\"item_18\"," + \
                                    "\"frule\",\"comnav\"," + \
                                    "\"processedfixroute\",\"processedfix\",\"processedroute\"," + \
                                    "\"flevel\"," + \
                                    "\"geom\",\"end_time\")"

                callsign = str(temp_1['C/S'])
                app_time = str(temp_1['APPTIME'])

                etd = str(temp_1['ETD'])
                if etd == 'None':
                    etd = 'null'
                else:
                    etd = "'" + etd + "'"
                atd = str(temp_1['ATD'])
                if atd == 'None':
                    atd = 'null'
                else:
                    atd = "'" + atd + "'"
                eta = str(temp_1['ETA'])
                if eta == 'None':
                    eta = 'null'
                else:
                    eta = "'" + eta + "'"
                ata = str(temp_1['ATA'])
                if ata == 'None':
                    ata = 'null'
                else:
                    ata = "'" + ata + "'"

                dep = str(temp_1['DEP'])
                dest = str(temp_1['DEST'])
                reg = str(temp_1['REG'])

                actype = str(temp_1['AcType'])
                # runway = str(temp_1['runway'])
                # sidstar = str(temp_1['sidstar'])
                item_18 = str(temp_1['Item18'])
                # op_type = str(temp_1['op_type'])
                frule = str(temp_1['FRULE'])
                comnav = str(temp_1['COMNAV'])
                processedfixroute = str(temp_1['ProcessedFixRoute'])
                processedfix = str(temp_1['ProcessedFix'])
                processedroute = str(temp_1['ProcessedRoute'])

                flight_id_1 = str(temp_1['FlightID'])
                flight_key_1 = str(temp_1['FlightKey'])

                postgres_sql_text = postgres_sql_text + " VALUES('" + callsign + "'," \
                                    + flight_id_1 + ",'" \
                                    + flight_key_1 + "','" \
                                    + app_time + "'," \
                                    + etd + "," \
                                    + atd + "," \
                                    + eta + "," \
                                    + ata + ",'" \
                                    + dep + "','" \
                                    + dest + "','" \
                                    + reg + "','" \
                                    + actype + "','" \
                                    + item_18 + "','" \
                                    + frule + "','" \
                                    + comnav + "','" \
                                    + processedfixroute + "','" \
                                    + processedfix + "','" \
                                    + processedroute + "','" \
                                    + flevel + "'," \
                                    + "ST_LineFromText('LINESTRING("
            else:
                break