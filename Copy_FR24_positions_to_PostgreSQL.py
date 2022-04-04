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
        columns = ', '.join('"{}"'.format(k) for k in keys)
        if table.schema:
            table_name = '{}.{}'.format(table.schema, table.name)
        else:
            table_name = table.name
        sql = 'COPY {} ({}) FROM STDIN WITH CSV'.format(table_name, columns)
        cur.copy_expert(sql=sql, file=s_buf)

filenames = next(walk('/Users/pongabha/Desktop/20210723_positions/'), (None, None, []))[2]  # [] if no file
print(filenames)

engine = create_engine('postgresql://postgres:password@localhost:5432/fr24')
df_list = list()

for filename in filenames:
    df = pandas.read_csv('/Users/pongabha/Desktop/20210723_positions/' + filename)
    df.insert(0, "date", filename[0:7] , True)
    df.insert(1, "flight_id", int(filename[9:17]), True)
    df_list.append(df)

combined_df = pandas.concat(df_list)

table_name = 'position'

conn_postgres = psycopg2.connect(user = "postgres",
                                  password = "password",
                                  host = "127.0.0.1",
                                  port = "5432",
                                  database = "fr24")
with conn_postgres:
    cur = conn_postgres.cursor()
    sql_query = "DROP TABLE IF EXISTS public." + table_name + ";"
    cur.execute(sql_query)
    conn_postgres.commit()

combined_df.to_sql(table_name, engine, schema='public', method=psql_insert_copy)

with conn_postgres:
    cur = conn_postgres.cursor()
    sql_query = "DELETE from \"position\" where date like 'posi%'; " + \
                "DROP TABLE IF EXISTS public." + table_name + '_geom' + ";" + \
    " SELECT *, ST_SetSRID(ST_MakePoint(longitude, latitude), 4326) " + \
    " AS geom INTO " + table_name + '_geom' + " FROM " + table_name + ";"
    cur.execute(sql_query)
    conn_postgres.commit()