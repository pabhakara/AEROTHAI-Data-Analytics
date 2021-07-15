import subprocess
import sqlalchemy  # Package for accessing SQL databases via Python
import pandas as pd
import re

def show_data(path='<file_name>.mdb', table='<table_name>'):
    tables = subprocess.check_output(["mdb-export", path, table])
    return tables.decode().split('\n')

def convert_df(path, table):
    d = show_data(path, table)
    columns = d[0].split(',')
    print(columns)
    data = []
    for line in d[1:]:
        print(line)
        line = re.sub(r",",".",line)
        print(line)
        data.append(line)
    #data = [i.split(',') for i in d[1:]]
    print(data)
    df = pd.DataFrame(columns=columns, data=data)
    return df

table_names = ['Waypoint']

for table_name in table_names:

    engine = sqlalchemy.create_engine("postgresql://postgres:password@localhost/airac_test")
    con = engine.connect()
    df = convert_df("/Users/pongabha/Dropbox/Workspace/AEROTHAI Data Analytics/NavData/airsimtech_native_2106/ASTNAV.mdb",
                table_name)
    #print(df)
    # Connect to database (Note: The package psychopg2 is required for Postgres to work with SQLAlchemy)
    df.to_sql(table_name, con)

    con.close()