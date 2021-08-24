import psycopg2

table_name = 'airways'

conn = psycopg2.connect(user = "postgres",
                        password = "password",
                        host = "127.0.0.1",
                        port = "5432",
                        database = "current_airac")

with conn:
    cur = conn.cursor()

    sql_query = "DROP TABLE IF EXISTS " + table_name + ";"

    cur.execute(sql_query)
    conn.commit()

    sql_query = "CREATE TABLE " + table_name \
                + '(route_identifier character varying, ' \
                + 'area_code character varying,' \
                + 'seqno integer,' \
                + 'geom geometry) ' \
                + 'WITH (OIDS=FALSE);' \
                + 'ALTER TABLE "' + table_name + '"' \
                + 'OWNER TO postgres;'

    print(sql_query)

    cur.execute(sql_query)
    conn.commit()

    sql_query = "SELECT route_identifier,area_code,seqno,waypoint_latitude,waypoint_longitude,inbound_distance " + \
                "FROM public.tbl_enroute_airways"

    # query
    cur.execute(sql_query)

    print(cur.rowcount)

    results = cur.fetchall()

    print("Total rows are:  ", len(results))

    # initialize parameters
    route_identifier = []
    area_code = []
    seqno = []
    waypoint_latitude = []
    waypoint_longitude = []
    inbound_distance = []

    # populate the parameters with the query results, row by row
    for row in results:
        route_identifier.append(row[0])
        area_code.append(row[1])
        seqno.append(row[2])
        waypoint_latitude.append(row[3])
        waypoint_longitude.append(row[4])
        inbound_distance.append(row[5])

    print("Number of rows: " + str(len(route_identifier)))
    num_of_ids = len(route_identifier)

    k = 0

    # initialize first INSERT query
    sql_text = "INSERT INTO " + table_name + \
               "(route_identifier," + \
                "area_code," + \
                "seqno," + \
                "geom) " + \
                "VALUES('" + \
                str(route_identifier[k]) + "','" + \
                str(area_code[k]) + "'," + \
                str(seqno[k]) + "," + \
                "ST_LineFromText('LINESTRING(" + \
               str(float(waypoint_longitude[k])) + " " + str(float(waypoint_latitude[k])) + ","

    while k < num_of_ids:
        if (inbound_distance[k] == 0) or \
                not(str(route_identifier[k]) == str(route_identifier[k+1])):

            sql_text = sql_text + str(float(waypoint_longitude[k])) + " " + str(float(waypoint_latitude[k])) + ")',4326));"

            print(sql_text)

            cur.execute(sql_text)
            conn.commit()

            k = k + 1

            sql_text = "INSERT INTO " + table_name + \
                       "(route_identifier," + \
                       "area_code," + \
                       "seqno," + \
                       "geom) " + \
                       "VALUES('" + \
                       str(route_identifier[k]) + "','" + \
                       str(area_code[k]) + "'," + \
                       str(seqno[k]) + "," + \
                       "ST_LineFromText('LINESTRING(" + \
                       str(float(waypoint_longitude[k])) + " " + str(float(waypoint_latitude[k])) + ","

            k = k + 1

        else:
            sql_text = sql_text + str(float(waypoint_longitude[k])) + " " + str(float(waypoint_latitude[k])) + ","
            k = k + 1

    print(sql_text)