import psycopg2

table_name = 'apch_legs'

try:
    # Setup Postgres DB connection
    conn = psycopg2.connect(user = "postgres",
                                  password = "password",
                                  host = "127.0.0.1",
                                  port = "5432",
                                  database = "airac_2021_06")

    cur = conn.cursor()

    sql_query = "DROP TABLE IF EXISTS " + table_name + ";"

    cur.execute(sql_query)
    conn.commit()

    sql_query = "CREATE TABLE " + table_name \
                + '(name character varying, ' \
                + 'airport_ident character varying,' \
                + 'rwy_designator character varying,' \
                + 'type character varying,' \
                + 'geom geometry) ' \
                + 'WITH (OIDS=FALSE);' \
                + 'ALTER TABLE "' + table_name + '"'  \
                + 'OWNER TO postgres;'

    #print(sql_query)

    cur.execute(sql_query)
    conn.commit()

    # sql_query = "select a.*, wp.Ident, " \
    #             "replace(wp.lat::text, ',', '.')," \
    #             "replace(wp.long::text, ',', '.') " \
    #             "from \"ATS\" a, \"Waypoint\" wp " \
    #             "where wp.id = a.idwayp " \
    #             "order by a.id ASC"

    sql_query = "SELECT terminal_id, " \
                "name, " \
                "airport_ident, " \
                "designator, " \
                "ident, " \
                "replace(waypointlat::text, ',', '.')," \
                "replace(waypointlong::text, ',', '.')," \
                "type, " \
                "trackcode " \
                "FROM approach_wp_geom " \
                "WHERE trackcode like \'IF\' or " \
                "trackcode like \'DF\' or " \
                "trackcode like \'CF\' or " \
                "trackcode like \'TF\' " \
                "order by termimalleg_id "

    print(sql_query)

    cur.execute(sql_query)

    print(cur.rowcount)

    results = cur.fetchall()

    print("Total rows are:  ", len(results))

    # initialize parameters
    terminal_id = []
    name = []
    airport_ident = []
    rwy_designator = []
    type = []
    waypoint_ident = []
    waypoint_lat = []
    waypoint_long = []
    trackcode = []

    # populate the parameters with the query results, row by row
    for row in results:
        terminal_id.append(row[0])
        name.append(row[1])
        airport_ident.append(row[2])
        rwy_designator.append(row[3])
        waypoint_ident.append(row[4])
        waypoint_lat.append(row[5])
        waypoint_long.append(row[6])
        type.append(row[7])
        trackcode.append(row[8])
    print("Number of rows: " + str(len(terminal_id)))
    num_of_ids = len(terminal_id)

    k = 1
    print(waypoint_lat[k])
    print(waypoint_long[k])

    # initialize first INSERT query
    sql_text =  "INSERT INTO " + table_name + " (name," +\
                "airport_ident,rwy_designator,type,geom) " +\
                "VALUES('" + str(name[k]) + "','" +\
                str(airport_ident[k]) + "','" + \
                str(rwy_designator[k]) + "','" + \
                str(type[k]) + "'," + \
                "ST_LineFromText('LINESTRING(" +\
                str(float(waypoint_long[k])) + " " + str(float(waypoint_lat[k])) + ","

    k = k + 1

    while k < num_of_ids:
        if  not(str(type[k]) == str(type[k+1])) or \
            not(str(terminal_id[k]) == str(terminal_id[k+1]))  :

            sql_text = sql_text + str(float(waypoint_long[k])) + " " + str(float(waypoint_lat[k])) + ")',4326));"

            print(sql_text)

            cur.execute(sql_text)
            conn.commit()

            k = k + 1

            sql_text = "INSERT INTO " + table_name + " (name," + \
                       "airport_ident,rwy_designator,type,geom) " + \
                       "VALUES('" + str(name[k]) + "','" + \
                       str(airport_ident[k]) + "','" + \
                       str(rwy_designator[k]) + "','" + \
                       str(type[k]) + "'," + \
                       "ST_LineFromText('LINESTRING(" + \
                       str(float(waypoint_long[k])) + " " + str(float(waypoint_lat[k])) + ","

            k = k + 1
        else:
            sql_text = sql_text + str(float(waypoint_long[k])) + " " + str(float(waypoint_lat[k])) + ","
            k = k + 1
    print(sql_text)

    cur.close()

except (Exception, psycopg2.DatabaseError) as error:
    print(error)

finally:
    if conn is not None:
        conn.close()


