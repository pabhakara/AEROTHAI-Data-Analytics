import psycopg2

table_name = 'runway_geom'

try:
    # Setup Postgres DB connection
    conn = psycopg2.connect(user = "postgres",
                                  password = "password",
                                  host = "127.0.0.1",
                                  port = "5432",
                                  database = "postgres")

    cur = conn.cursor()

    sql_query = "DROP TABLE IF EXISTS " + table_name + ";"

    cur.execute(sql_query)
    conn.commit()

    sql_query = "CREATE TABLE " + table_name \
                + '(id integer,'\
                + 'idairport integer,'\
                + 'designator character varying(3),'\
                + 'length integer,'\
                + 'width integer,'\
                + 'trueheading integer,'\
                + 'variation integer,'\
                + 'latbegin character varying(8),'\
                + 'longbegin character varying(9),'\
                + 'latend character varying(8),'\
                + 'longend character varying(9),'\
                + 'ilsfrequency character varying(6),'\
                + 'ilscategory integer,' \
                + 'ilsangle character varying(4),'\
                + 'ilsocalisercourse integer,'\
                + 'ilsident character varying(4),'\
                + 'ilsdme character varying(1),'\
                + 'geom geometry) ' \
                + 'WITH (OIDS=FALSE);' \
                + 'ALTER TABLE "' + table_name + '"'  \
                + 'OWNER TO postgres;'

    #print(sql_query)

    #cur.execute(sql_query)
    #conn.commit()

    sql_query = "select a.*, rw.ident, " \
                "replace(rw.latbegin::text, ',', '.')," \
                "replace(rw.longbegin::text, ',', '.') " \
                "replace(rw.latend::text, ',', '.')," \
                "replace(rw.longend::text, ',', '.') " \
                "from ats a, runway rw " \
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
    sql_text =  "INSERT INTO " + table_name + " (ats_route_id," +\
                "sub_route_id,geom) " +\
                "VALUES('" + str(ats_route_id[k]) + "'," +\
                str(sub_route_id) + ",ST_LineFromText('LINESTRING(" +\
                str(float(waypoint_long[k])) + " " + str(float(waypoint_lat[k])) + ","


    k = k + 1

    while k < num_of_ids:
        if (terminate[k] == 'Y') or not(str(ats_route_id[k]) == str(ats_route_id[k+1])):

            sql_text = sql_text + str(float(waypoint_long[k])) + " " + str(float(waypoint_lat[k])) + ")',4326));"

            print(sql_text)

            cur.execute(sql_text)
            conn.commit()

            if not (str(ats_route_id[k] == str(ats_route_id[k + 1]))):
                sub_route_id = 1
            else:
                sub_route_id = sub_route_id + 1

            k = k + 1

            sql_text = "INSERT INTO " + table_name + " (ats_route_id," + \
                       "sub_route_id,geom) " + \
                       "VALUES('" + str(ats_route_id[k]) + "'," + \
                       str(sub_route_id) + ",ST_LineFromText('LINESTRING(" + \
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


