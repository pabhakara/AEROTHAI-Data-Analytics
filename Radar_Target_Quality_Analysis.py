import psycopg2
import datetime
import mysql.connector
import math
import time
import matplotlib.pyplot as plt
import numpy as np

from mysql.connector import Error


def getFieldAsList(fieldname,tablename):
    #additional_condition = " where dist_from_last_position < 4"
    additional_condition = " "
    cursor_postgres.execute("select " + field + " from " + table + additional_condition)
    id_data = cursor_postgres.fetchall()
    id_list = []
    for index in range(len(id_data)):
        id_list.append(id_data[index][0])
    return id_list

def plotHistField(field,table):
    list2 = getFieldAsList(field, table)
    list2_array = np.asarray(list2)
    plt.hist(list2_array,bins=100)
    plt.title(field)
    plt.show()

t = time.time()

# Try to connect to the local PostGresSQL database in which we will store our flight trajectories coupled with FPL data.
try:
    conn_postgres = psycopg2.connect(user = "postgres",
                                  password = "password",
                                  host = "127.0.0.1",
                                  port = "5432",
                                  database = "flight_track_5s")

    cursor_postgres = conn_postgres.cursor()

    table = 'target_2021_02_02'

    #field = 'dist_from_last_position'

    field = 'latitude'


    # field = 'speed'

    #
    #field = 'actual_flight_level'
    #plotHistField(field, table)

    plotHistField(field, table)


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