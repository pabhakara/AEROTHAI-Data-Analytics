import psycopg2
import pandas, csv
from io import StringIO
from sqlalchemy import create_engine
from os import walk

def psql_insert_copy(table, conn, keys, data_iter):
    dbapi_conn = conn.connection
    with dbapi_conn.cursor() as cur:
        s_buf = StringIO()
        writer = csv.writer(s_buf)
        writer.writerows(data_iter)
        s_buf.seek(0)
        columns = ', '.join(f'"{k}"'for k in keys)
        if table.schema:
            table_name = f"{table.schema}.{table.name}"
        else:
            table_name = table.name
        #sql = 'COPY {} ({}) FROM STDIN WITH CSV'.format(table_name, columns)
        sql = f"COPY {table_name} ({columns}) FROM STDIN WITH CSV"
        cur.copy_expert(sql=sql, file=s_buf)

yyyymmdd = '20220321'
filenames = ['20220321 2.txt']
print(filenames)

engine = create_engine('postgresql://postgres:password@localhost:5432/adsb')
df_list = list()


for filename in filenames:
    df = pandas.read_csv(f'/Users/pongabha/Dropbox/Workspace/Surveillance Enhancement/Data/{filename}',low_memory=False)
    print(df)
    df.insert(0, "date", filename[0:7] , True)
    df_list.append(df)

combined_df = pandas.concat(df_list)
print(combined_df)

table_name = 'adsb_' + yyyymmdd

conn_postgres = psycopg2.connect(user = "postgres",
                                  password = "password",
                                  host = "127.0.0.1",
                                  port = "5432",
                                  database = "adsb")
with conn_postgres:
    cur = conn_postgres.cursor()
    sql_query = "DROP TABLE IF EXISTS public." + table_name + ";"
    cur.execute(sql_query)
    conn_postgres.commit()

combined_df = combined_df.iloc[: , :-1]
combined_df.to_sql(table_name, engine, schema='public', method=psql_insert_copy)

with conn_postgres:
    sql_query = f"drop table if exists \"adsb_{yyyymmdd}_geom\"; " + \
    "SELECT *, ST_SetSRID(ST_MakePoint(\"15:wgs_lon\",\"14:wgs_lat\"),4326) AS geom " + \
    f"into \"adsb_{yyyymmdd}_geom\" " + \
    f"from \"adsb_{yyyymmdd}\";"
    cur.execute(sql_query)
    conn_postgres.commit()