import psycopg2
import datetime
import matplotlib.pyplot as plt
import mysql.connector
import math

from mysql.connector import Error

# Try to connect to the local PostGresSQL database in which we will store our flight trajectories mapped with FPL data.
try:
    conn_postgres = psycopg2.connect(user = "postgres",
                                  password = "password",
                                  host = "127.0.0.1",
                                  port = "5432",
                                  database = "postgres")

    cursor_postgres = conn_postgres.cursor()


    # create the table name that will store the radar track
    year_month = "2019_05"
    flight_id = 1;

    # Try to connect to the local MySQL database which contains flight plan data and radar target data.
    try:
        conn_mysql = mysql.connector.connect(host='localhost',
                                             database='flight',
                                             user='root',
                                             password='password')
        if conn_mysql.is_connected():

            # Select flight plan data and radar target data from MySQL database
            mysql_query = "SELECT a.flight_id,b.app_time,a.dep,a.dest,a.actype,a.ftype,a.op_type,a.etd,a.atd,a.eta,a.ata,a.flevel," + \
            "b.latitude,b.longitude,b.actual_flight_level,b.cdm," + \
            "b.sector,a.callsign,a.route,a.pbn_type,a.comnav," + \
            "a.sidstar,a.runway,a.frule,a.reg,a.entry_flevel,a.maintain_flevel,a.exit_flevel " + \
            "from " + year_month + "_radar a, target_" + year_month + " b " + \
            "where (a.flight_id = b.flight_id) and a.flight_id = " + flight_id + " "\
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


            distance_from_last_position_1 = temp_1['dist_from_last_postion']
            distance_from_last_position_2 = temp_2['dist_from_last_postion']

            flight_id_1 = str(temp_1['flight_id'])
            flight_id_2 = str(temp_2['flight_id'])


            actual_flight_level_1 = str(temp_1['actual_flight_level'])
            actual_flight_level_2 = str(temp_2['actual_flight_level'])

            while k < num_of_records - 1:
                k = k + 1
                temp_1 = record[k]
                temp_2 = record[k + 1]

                actual_flight_level_1 = str(temp_1['actual_flight_level'])
                actual_flight_level_2 = str(temp_2['actual_flight_level'])

                distance_from_last_position_1 = temp_1['dist_from_last_postion']
                distance_from_last_position_2 = temp_2['dist_from_last_postion']


                if k < num_of_records:

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