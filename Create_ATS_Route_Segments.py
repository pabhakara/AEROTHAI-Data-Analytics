import psycopg2

table_name = 'ats_route_segments'

conn = psycopg2.connect(user = "postgres",
                        password = "password",
                        host = "127.0.0.1",
                        port = "5432",
                        database = "airac_2021_06")

with conn:
    cur = conn.cursor()

    sql_query = "DROP TABLE IF EXISTS " + table_name + ";"

    cur.execute(sql_query)
    conn.commit()

    sql_query = "CREATE TABLE " + table_name \
                + '(ats_route_id character varying, ' \
                + 'sub_route_id character varying,' \
                + 'wp_start character varying,' \
                + 'wp_end character varying,' \
                + 'geom geometry) ' \
                + 'WITH (OIDS=FALSE);' \
                + 'ALTER TABLE "' + table_name + '"'  \
                + 'OWNER TO postgres;'

    #print(sql_query)

    cur.execute(sql_query)
    conn.commit()

    sql_query = "select a.*, wp.Ident, " \
                "replace(wp.lat::text, ',', '.')," \
                "replace(wp.long::text, ',', '.') " \
                "from \"ATS\" a, \"Waypoint\" wp " \
                "where wp.id = a.idwayp " \
                "order by a.id ASC"


    print(sql_query)

    cur.execute(sql_query)

    print(cur.rowcount)

    results = cur.fetchall()

    print("Total rows are:  ", len(results))

    # initialize parameters
    ID = []
    idats = []
    ats_route_id = []
    idwayp = []
    terminate = []
    waypoint_ident = []
    waypoint_lat = []
    waypoint_long = []

    # populate the parameters with the query results, row by row
    for row in results:
        ID.append(row[0])
        idats.append(row[1])
        ats_route_id.append(row[2])
        idwayp.append(row[3])
        terminate.append(row[4])
        waypoint_ident.append(row[5])
        waypoint_lat.append(row[6])
        waypoint_long.append(row[7])
    print("Number of rows: " + str(len(ID)))
    num_of_ids = len(ID)

    k = 1
    sub_route_id = 1

    print(waypoint_lat[k])
    print(waypoint_long[k])

    # initialize first INSERT query
    sql_text =  "INSERT INTO " + table_name + " (ats_route_id," + \
                "sub_route_id,wp_start,wp_end,geom) " +\
                "VALUES('" + str(ats_route_id[k]) + "'," + \
                str(sub_route_id) + ",'" + \
                str(waypoint_ident[k]) + "','" + \
                str(waypoint_ident[k+1]) + "'" + \
                ",ST_LineFromText('LINESTRING(" + \
                str(float(waypoint_long[k])) + " " + str(float(waypoint_lat[k])) + "," + \
                str(float(waypoint_long[k+1])) + " " + str(float(waypoint_lat[k+1])) + ")',4326));"
    print(sql_text)

    cur.execute(sql_text)
    conn.commit()

    while k < num_of_ids:
        if (terminate[k+1] == 'Y'):
            k = k + 1
            if not (str(ats_route_id[k] == str(ats_route_id[k + 1]))):
                sub_route_id = 1
            else:
                sub_route_id = sub_route_id + 1
        else:
            k = k + 1
            sql_text = "INSERT INTO " + table_name + " (ats_route_id," + \
                   "sub_route_id,wp_start,wp_end,geom) " + \
                   "VALUES('" + str(ats_route_id[k]) + "'," + \
                   str(sub_route_id) + ",'" + \
                   str(waypoint_ident[k]) + "','" + \
                   str(waypoint_ident[k + 1]) + "'" + \
                   ",ST_LineFromText('LINESTRING(" + \
                   str(float(waypoint_long[k])) + " " + str(float(waypoint_lat[k])) + "," + \
                   str(float(waypoint_long[k + 1])) + " " + str(float(waypoint_lat[k + 1])) + ")',4326));"
            print(sql_text)
            cur.execute(sql_text)
            conn.commit()