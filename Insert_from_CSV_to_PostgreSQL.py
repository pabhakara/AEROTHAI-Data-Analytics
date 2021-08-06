import psycopg2
import csv

# Try to connect to the local PostGresSQL database in which we will store our FPL data.
conn_postgres = psycopg2.connect(user = "postgres",
                                  password = "password",
                                  host = "127.0.0.1",
                                  port = "5432",
                                  database = "los_2021_07")
with conn_postgres:
    cur = conn_postgres.cursor()

    table_name = 'fdms_flight_plan_processed'

    sql_query = "DROP TABLE public.fdms_flight_plan_processed; " + \
    "CREATE TABLE IF NOT EXISTS public.fdms_flight_plan_processed " + \
    "(\"STATUS\" text COLLATE pg_catalog.\"default\", " + \
    "\"DOF\" text, " + \
    "\"FlightType\" int, " + \
    "\"REG\" text COLLATE pg_catalog.\"default\", " + \
    "\"MessageType\" text COLLATE pg_catalog.\"default\", " + \
    "\"C/S\" text COLLATE pg_catalog.\"default\", " + \
    "\"AcType\" text COLLATE pg_catalog.\"default\", " + \
    "\"DEP\" text COLLATE pg_catalog.\"default\", " + \
    "\"ETD\" timestamp, " + \
    "\"ATD\" timestamp, " + \
    "\"Route\" text COLLATE pg_catalog.\"default\", " + \
    "\"DEST\" text COLLATE pg_catalog.\"default\", " + \
    "\"ETA\" timestamp, " + \
    "\"ATA\" timestamp, " + \
    "\"No\" text COLLATE pg_catalog.\"default\", " + \
    "\"Squawk\" text COLLATE pg_catalog.\"default\", " + \
    "\"FRULE\" text COLLATE pg_catalog.\"default\", " + \
    "\"WTURB\" text COLLATE pg_catalog.\"default\", " + \
    "\"COMNAV\" text COLLATE pg_catalog.\"default\", " + \
    "\"SPEED\" text COLLATE pg_catalog.\"default\", " + \
    "\"FLEVEL\" text COLLATE pg_catalog.\"default\", " + \
    "\"EET\" text COLLATE pg_catalog.\"default\", " + \
    "\"INBOUND\" timestamp, " + \
    "\"OUTBOUND\" timestamp, " + \
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
        # cur.copy_from(f, 'fdms_flight_plan_processed', sep=',')
        reader = csv.reader(f)
        for row in reader:
            STATUS = row[0]
            DOF = row[1]
            FlightType = row[2]
            REG = row[3]
            MessageType = row[4]
            Callsign = row[5]
            AcType = row[6]
            DEP = row[7]
            ETD = row[8]
            if ETD == '':
                ETD = 'null'
            else:
                ETD = "'" + ETD + "'"
            ATD = row[9]
            if ATD == '':
                ATD = 'null'
            else:
                ATD = "'" + ATD + "'"
            Route = row[10]
            DEST = row[11]
            ETA = row[12]
            if ETA == '':
                ETA = 'null'
            else:
                ETA = "'" + ETA + "'"
            ATA = row[13]
            if ATA == '':
                ATA = 'null'
            else:
                ATA = "'" + ATA + "'"
            No = row[14]
            Squawk = row[15]
            FRULE = row[16]
            WTURB = row[17]
            COMNAV = row[18]
            SPEED = row[19]
            FLEVEL = row[20]
            EET = row[21]
            INBOUND = row[22]
            if INBOUND == '':
                INBOUND = 'null'
            else:
                INBOUND = "'" + INBOUND + "'"
            OUTBOUND = row[23]
            if OUTBOUND == '':
                OUTBOUND = 'null'
            else:
                OUTBOUND = "'" + OUTBOUND + "'"
            ALTDEST = row[24]
            ALTDEST2 = row[25]
            DLA = row[26]
            CHG = row[27]
            CNL = row[28]

            sql_query = "INSERT INTO \"" + table_name + "\" " + \
            "(\"STATUS\"," + \
            "\"DOF\"," + \
            "\"FlightType\"," + \
            "\"REG\"," + \
            "\"MessageType\"," + \
            "\"C/S\"," + \
            "\"AcType\"," + \
            "\"DEP\"," + \
            "\"ETD\"," + \
            "\"ATD\"," + \
            "\"Route\"," + \
            "\"DEST\"," + \
            "\"ETA\"," + \
            "\"ATA\"," + \
            "\"No\"," + \
            "\"Squawk\"," + \
            "\"FRULE\"," + \
            "\"WTURB\"," + \
            "\"COMNAV\"," + \
            "\"SPEED\"," + \
            "\"FLEVEL\"," + \
            "\"EET\"," + \
            "\"INBOUND\"," + \
            "\"OUTBOUND\"," + \
            "\"ALTDEST\"," + \
            "\"ALTDEST2\"," + \
            "\"DLA\"," + \
            "\"CHG\"," + \
            "\"CNL\")"

            sql_query = sql_query + " VALUES('"  \
                + STATUS + "','" \
                + DOF + "','" \
                + FlightType + "','" \
                + REG + "','" \
                + MessageType + "','" \
                + Callsign + "','" \
                + AcType + "','" \
                + DEP + "'," \
                + ETD + "," \
                + ATD + ",'" \
                + Route + "','" \
                + DEST + "'," \
                + ETA + "," \
                + ATA + ",'" \
                + No + "','" \
                + Squawk + "','" \
                + FRULE + "','" \
                + WTURB + "','" \
                + COMNAV + "','" \
                + SPEED + "','" \
                + FLEVEL + "','" \
                + EET + "'," \
                + INBOUND + "," \
                + OUTBOUND + ",'" \
                + ALTDEST + "','" \
                + ALTDEST2 + "','" \
                + DLA + "','" \
                + CHG + "','" \
                + CNL + "')"
            cur.execute(sql_query)
            conn_postgres.commit()