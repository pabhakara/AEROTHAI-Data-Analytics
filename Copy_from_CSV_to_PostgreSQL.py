import psycopg2

# Try to connect to the local PostGresSQL database in which we will store our FPL data.
conn_postgres = psycopg2.connect(user = "postgres",
                                  password = "password",
                                  host = "127.0.0.1",
                                  port = "5432",
                                  database = "los_2021_07")
with conn_postgres:
    cur = conn_postgres.cursor()

    sql_query = "DROP TABLE public.fdms_flight_plan_processed; " + \
    "CREATE TABLE IF NOT EXISTS public.fdms_flight_plan_processed " + \
    "(\"STATUS\" text COLLATE pg_catalog.\"default\", " + \
    "\"DOF\" text, " + \
    "\"FlightType\" text COLLATE pg_catalog.\"default\", " + \
    "\"REG\" text COLLATE pg_catalog.\"default\", " + \
    "\"MessageType\" text COLLATE pg_catalog.\"default\", " + \
    "\"C/S\" text COLLATE pg_catalog.\"default\", " + \
    "\"AcType\" text COLLATE pg_catalog.\"default\", " + \
    "\"DEP\" text COLLATE pg_catalog.\"default\", " + \
    "\"ETD\" timestamp, " + \
    "\"ATD\" text, " + \
    "\"Route\" text COLLATE pg_catalog.\"default\", " + \
    "\"DEST\" text COLLATE pg_catalog.\"default\", " + \
    "\"ETA\" timestamp, " + \
    "\"ATA\" text, " + \
    "\"No\" text COLLATE pg_catalog.\"default\", " + \
    "\"Squawk\" text COLLATE pg_catalog.\"default\", " + \
    "\"FRULE\" text COLLATE pg_catalog.\"default\", " + \
    "\"WTURB\" text COLLATE pg_catalog.\"default\", " + \
    "\"COMNAV\" text COLLATE pg_catalog.\"default\", " + \
    "\"SPEED\" text COLLATE pg_catalog.\"default\", " + \
    "\"FLEVEL\" text COLLATE pg_catalog.\"default\", " + \
    "\"EET\" text COLLATE pg_catalog.\"default\", " + \
    "\"INBOUND\" text, " + \
    "\"OUTBOUND\" text, " + \
    "\"ALTDEST\" text COLLATE pg_catalog.\"default\", " + \
    "\"ALTDEST2\" text COLLATE pg_catalog.\"default\", " + \
    "\"DLA\" text COLLATE pg_catalog.\"default\", " + \
    "\"CHG\" text COLLATE pg_catalog.\"default\", " + \
    "\"CNL\" text COLLATE pg_catalog.\"default\" " + \
    ") WITH (OIDS=FALSE) " + \
    "TABLESPACE " + \
    "pg_default; "

    cur.execute(sql_query)

    conn_postgres.commit()

    with open('/Users/pongabha/Dropbox/Workspace/AEROTHAI Data Analytics/Data samples/FPL/FPL Feb2021/20210205_new.csv', 'r') as f:
        # Notice that we don't need the `csv` module.
        cur.copy_from(f, 'fdms_flight_plan_processed', sep=',')

    conn_postgres.commit()