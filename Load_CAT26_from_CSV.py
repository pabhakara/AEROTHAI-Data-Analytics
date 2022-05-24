import pandas as pd
from sqlalchemy import create_engine
from os import walk
import psycopg2.extras
import psycopg2

path = "/Users/pongabha/Dropbox/Workspace/Surveillance Enhancement/Data/CAT62/20210921/"
engine = create_engine('postgresql://postgres:password@localhost:5432/adsb')

filenames = next(walk(path), (None, None, []))[2]

df = pd.read_csv(path + filenames[0])

for filename in filenames[1:]:
    df_temp = pd.read_csv(path + filename)
    df = pd.concat([df,df_temp])

#print(df)

#df.drop(['Mode 5 Data reports and Extended Mode 1 Code.Mode 5 PIN /National Origin/ Mission Code.Spare'], axis = 1)

selected_column_list = ['Time of Track Information.Time of Track Information',
                        'Measured Information.Sensor Identification.SIC',
                        'Track Number.Track Number',
                        'Track Mode 3/A Code.Reply',
                        'Calculated Position In WGS-84 Co-ordinates.Latitude',
                        'Calculated Position In WGS-84 Co-ordinates.Longitude',
                        'Calculated Track Geometric Altitude.Altitude',
                        'Calculated Rate Of Climb/Descent.Rate of Climb/Descent',
                        'Calculated Track Barometric Altitude.QNH',
                        'Aircraft Derived Data.Position.Latitude',
                        'Aircraft Derived Data.Position.Longitude',
                        'Aircraft Derived Data.Selected Altitude.Altitude',
                        'Aircraft Derived Data.Final State Selected Altitude.Altitude',
                        'Aircraft Derived Data.Barometric Vertical Rate.Barometric Vertical Rate',
                        'Aircraft Derived Data.Magnetic Heading.Magnetic Heading',
                        'Aircraft Derived Data.Target Address.Target Address',
                        'Aircraft Derived Data.Ident.Ident',
                        'Aircraft Derived Data.Indicated Airspeed.Indicated Airspeed',
                        'Aircraft Derived Data.True Airspeed.True Airspeed',
                        'Flight Plan Related Data.Current Cleared Flight Level.CFL',
                        'Flight Plan Related Data.Callsign.Callsign',
                        'Flight Plan Related Data.Departure Airport.Departure Airport',
                        'Flight Plan Related Data.Destination Airport.Destination Airport',
                        'Flight Plan Related Data.Type of Aircraft.Aircraft type',
                        'Flight Plan Related Data.Wake Turbulence Category.Wake Turbulence Category',
                        'Measured Flight Level.Measured Flight Level']

print(selected_column_list)

df_new = df[selected_column_list]

print(df_new)

table_name = 'cat62_20210921'

# Try to connect to the local PostGresSQL database in which we will store our flight trajectories coupled with FPL data.
conn_postgres = psycopg2.connect(user="postgres",
                                 password="password",
                                 host="127.0.0.1",
                                 port="5432",
                                 database="adsb")
with conn_postgres:
    cursor_postgres = conn_postgres.cursor(cursor_factory=psycopg2.extras.DictCursor)
    postgres_sql_text = "DROP TABLE IF EXISTS " + table_name + ";"
    cursor_postgres.execute(postgres_sql_text)
    conn_postgres.commit()

    df_new.to_sql(table_name, engine)

    postgres_sql_text = "drop table if exists \"" + table_name + "_geom\";" \
            "SELECT *, " \
            "ST_SetSRID(ST_MakePoint(\"Aircraft Derived Data.Position.Longitude\", " \
            "\"Aircraft Derived Data.Position.Latitude\"),4326)" \
            " AS aircraft_derived_geom," \
            "ST_SetSRID(ST_MakePoint(\"Calculated Position In WGS-84 Co-ordinates.Longitude\", " \
            "\"Calculated Position In WGS-84 Co-ordinates.Latitude\"), 4326)" \
            " AS calculated_geom " \
            " INTO " + table_name + "_geom from \"" + table_name + "\";"

    cursor_postgres.execute(postgres_sql_text)
    conn_postgres.commit()