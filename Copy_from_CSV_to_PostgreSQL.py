import psycopg2

# Try to connect to the local PostGresSQL database in which we will store our FPL data.
conn_postgres = psycopg2.connect(user = "postgres",
                                  password = "password",
                                  host = "127.0.0.1",
                                  port = "5432",
                                  database = "temp")
with conn_postgres:
    cur = conn_postgres.cursor()

    sql_query = "DROP TABLE IF EXISTS public.airline_code; " + \
    "CREATE TABLE IF NOT EXISTS public.airline_code " + \
    "(\"iata\" text COLLATE pg_catalog.\"default\", " + \
    "\"icao\" text COLLATE pg_catalog.\"default\", " + \
    "\"name\" text COLLATE pg_catalog.\"default\", " + \
    "\"callsign\" text COLLATE pg_catalog.\"default\", " + \
    "\"country\" text COLLATE pg_catalog.\"default\", " + \
    "\"remark\" text COLLATE pg_catalog.\"default\" " + \
    ") WITH (OIDS=FALSE) " + \
    "TABLESPACE " + \
    "pg_default; "

    cur.execute(sql_query)

    conn_postgres.commit()

    with open('/Users/pongabha/Desktop/airline_code.csv', 'r') as f:
        # Notice that we don't need the `csv` module.
        cur.copy_from(f, 'airline_code', sep=',')
    conn_postgres.commit()