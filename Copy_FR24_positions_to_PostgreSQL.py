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

filenames = next(walk(f'/Users/pongabha/Dropbox/Workspace/Surveillance Enhancement/FR24/{yyyymmdd}_positions/'), (None, None, []))[2]  # [] if no file
print(filenames)

engine = create_engine('postgresql://postgres:password@localhost:5432/fr24')
df_list = list()

for filename in filenames:
    df = pandas.read_csv(f'/Users/pongabha/Dropbox/Workspace/Surveillance Enhancement/FR24/{yyyymmdd}_positions/' + filename)
    df.insert(0, "date", filename[0:8] , True)
    df.insert(1, "flight_id", int(filename[9:18]), True)
    df_list.append(df)

combined_df = pandas.concat(df_list)

table_name = f'position_{yyyymmdd}'

conn_postgres = psycopg2.connect(user = "postgres",
                                  password = "password",
                                  host = "127.0.0.1",
                                  port = "5432",
                                  database = "fr24")
with conn_postgres:
    cur = conn_postgres.cursor()
    sql_query = f"DROP TABLE IF EXISTS public.{table_name};"
    cur.execute(sql_query)
    conn_postgres.commit()

combined_df.to_sql(table_name, engine, schema='public', method=psql_insert_copy)

with conn_postgres:
    cur = conn_postgres.cursor()

    sql_query = f"DELETE from \"{table_name}\" where date like 'posi%'; " + \
                f"DROP TABLE IF EXISTS public.{table_name}_geom;" + \
                f"SELECT *, CAST(flight_id AS VARCHAR) AS flight_id_txt, ST_SetSRID(ST_MakePoint(longitude, latitude), 4326) " + \
                f"AS geom INTO {table_name}_geom FROM {table_name};"\
                f"ALTER TABLE public.position_{yyyymmdd}_geom "\
                f"DROP COLUMN flight_id;" \
                f"ALTER TABLE public.position_{yyyymmdd}_geom " \
                f"RENAME COLUMN flight_id_txt TO flight_id;"

    cur.execute(sql_query)
    conn_postgres.commit()

    sql_query = f"DROP TABLE IF EXISTS public.position_{yyyymmdd}_geom_mapped; "\
                f"SELECT t.*, "\
                f"f.aircraft_id, f.reg, f.equip, f.callsign, f.flight, f.schd_from, f.schd_to, f.real_to, f.reserved "\
                f"INTO public.position_{yyyymmdd}_geom_mapped "\
                f"FROM public.position_{yyyymmdd}_geom t "\
                f"LEFT JOIN public.flight_{yyyymmdd} f "\
                f"ON t.flight_id = f.flight_id; "

    cur.execute(sql_query)
    conn_postgres.commit()

    sql_query = f"DROP TABLE IF EXISTS {table_name}_geom;" + \
                f"DROP TABLE IF EXISTS {table_name};" + \
                f"ALTER TABLE {table_name}_geom_mapped RENAME TO {table_name}_geom;"
    cur.execute(sql_query)
    conn_postgres.commit()

    sql_query = f"DROP TABLE IF EXISTS {table_name}_geom_mapped;" + \
                "SELECT p.*, " + \
                "a1.airport_identifier as dep, " + \
                "a2.airport_identifier as dest, " + \
                "to_timestamp(snapshot_id::INT - 7 * 60 * 60)::timestamp without time zone at time zone " + \
                "\'Etc/UTC\' as position_time " + \
                f"INTO {table_name}_geom_mapped " + \
                f"FROM {table_name}_geom p " + \
                f"LEFT JOIN flight_{yyyymmdd} f " + \
                "ON f.flight_id = p.flight_id " + \
                "LEFT JOIN airports a1 " + \
                "ON a1.iata_ata_designator = f.schd_from " + \
                "LEFT JOIN airports a2 " + \
                "ON a2.iata_ata_designator = f.schd_to;"

    cur.execute(sql_query)
    conn_postgres.commit()

    sql_query = f"DROP TABLE IF EXISTS {table_name}_geom;" + \
                f"DROP TABLE IF EXISTS {table_name};" + \
                f"ALTER TABLE {table_name}_geom_mapped RENAME TO {table_name};"
    cur.execute(sql_query)
    conn_postgres.commit()



