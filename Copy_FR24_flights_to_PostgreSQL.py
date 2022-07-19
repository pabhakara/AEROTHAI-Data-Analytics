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

yyyymmdd = '20191225'
year_month_day = "2019_12_25"

filenames = [ yyyymmdd +'_flights.csv']
print(filenames)

engine = create_engine('postgresql://postgres:password@localhost:5432/fr24')
df_list = list()

path = "/Users/pongabha/Dropbox/Workspace/Surveillance Enhancement/FR24/"

for filename in filenames:
    df = pandas.read_csv(path + filename)
    print(df)
    df.insert(0, "date", filename[0:7] , True)
    df_list.append(df)

combined_df = pandas.concat(df_list)
print(combined_df)
table_name = 'flight_' + yyyymmdd

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
    sql_query = f"DROP TABLE IF EXISTS flight_{yyyymmdd}_temp; " + \
                f"SELECT *, CAST(flight_id AS text) flight_id_txt " + \
                f"INTO flight_{yyyymmdd}_temp " + \
                f"FROM flight_{yyyymmdd};" + \
                f"ALTER TABLE flight_{yyyymmdd}_temp " \
                f"DROP COLUMN flight_id;" \
                f"ALTER TABLE flight_{yyyymmdd}_temp " \
                f"RENAME COLUMN flight_id_txt TO flight_id; " \
                f"DROP TABLE IF EXISTS flight_{yyyymmdd}; " \
                f"ALTER TABLE flight_{yyyymmdd}_temp " \
                f"RENAME TO flight_{yyyymmdd};"
    cur.execute(sql_query)
    conn_postgres.commit()
