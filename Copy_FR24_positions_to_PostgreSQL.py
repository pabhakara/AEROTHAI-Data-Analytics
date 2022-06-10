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

filenames = next(walk('/Users/pongabha/Dropbox/Workspace/Surveillance Enhancement/FR24/20191225_positions/'), (None, None, []))[2]  # [] if no file
print(filenames)

engine = create_engine('postgresql://postgres:password@localhost:5432/fr24')
df_list = list()

for filename in filenames:
    df = pandas.read_csv('/Users/pongabha/Dropbox/Workspace/Surveillance Enhancement/FR24/20191225_positions/' + filename)
    df.insert(0, "date", filename[0:8] , True)
    df.insert(1, "flight_id", int(filename[9:18]), True)
    df_list.append(df)

combined_df = pandas.concat(df_list)

table_name = 'position_20191225'

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

    sql_query = "DELETE from \"" + table_name + "\" where date like 'posi%'; " + \
                "DROP TABLE IF EXISTS public." + table_name + '_geom' + ";" + \
                "SELECT *, CAST(flight_id AS VARCHAR) AS flight_id_txt , ST_SetSRID(ST_MakePoint(longitude, latitude), 4326) " + \
                "AS geom INTO " + table_name + '_geom' + " FROM " + table_name + ";"

    cur.execute(sql_query)
    conn_postgres.commit()

    sql_query = "DROP TABLE IF EXISTS "  + table_name + '_geom_mapped;' + \
                "SELECT p. *, f.aircraft_id, f.reg, f.equip, f.callsign, f.schd_from, f.schd_to, " + \
                "a1.airport_identifier as dep, " + \
                "a2.airport_identifier as dest, " + \
                "to_timestamp(snapshot_id::INT - 7 * 60 * 60)::timestamp without time zone at time zone " + \
                "\'Etc/UTC\' as position_time " + \
                "INTO " + table_name + "_geom_mapped " + \
                "FROM " + table_name + "_geom p " + \
                "LEFT JOIN flight_20191225 f " + \
                "ON f.flight_id = p.flight_id_txt " + \
                "LEFT JOIN airports a1 " + \
                "ON a1.iata_ata_designator = f.schd_from " + \
                "LEFT JOIN airports a2 " + \
                "ON a2.iata_ata_designator = f.schd_to;"

    cur.execute(sql_query)
    conn_postgres.commit()

    sql_query = "DROP TABLE IF EXISTS " + table_name + '_geom;' + \
                "DROP TABLE IF EXISTS " + table_name + ';' + \
                "ALTER TABLE " + table_name + "_geom_mapped RENAME TO " + table_name + ";"
    cur.execute(sql_query)
    conn_postgres.commit()

