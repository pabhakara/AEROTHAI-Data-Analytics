import psycopg2
import psycopg2.extras
import pandas as pds
import geopandas
import matplotlib.pyplot as plt

from sqlalchemy import create_engine

try:
    conn_postgres = psycopg2.connect(user = "postgres",
                                  password = "password",
                                  host = "127.0.0.1",
                                  port = "5432",
                                  database = "postgres")

    # Create an engine instance

    alchemyEngine = create_engine('postgresql+psycopg2://postgres:@127.0.0.1', pool_recycle=3600);

    # Connect to PostgreSQL server

    dbConnection = alchemyEngine.connect();

    # Read data from PostgreSQL database table and load into a DataFrame instance

    df = pds.read_sql("select * from \"radar_track_2019_z\"", dbConnection)

    pds.set_option('display.expand_frame_repr', False)

    # Print the DataFrame

    #print(df.query('reg.str.startswith("HS")'))

    #print(df.query('pbn_type.str.contains("S2") and reg.str.startswith("HS")'))

    #print(df.describe())

    General = df.query('dest.str.startswith("VT") and dep.str.startswith("VT")')

    print("-------------- \n")

    print(General.describe())

    factor = pds.qcut(General["track_length"], [0, 0.25, 0.5, 0.75, 1],duplicates='drop')

    print("-------------- \n")

    print(factor)

except (Exception, psycopg2.Error) as error :
    print ("Error while connecting to PostgreSQL", error)
