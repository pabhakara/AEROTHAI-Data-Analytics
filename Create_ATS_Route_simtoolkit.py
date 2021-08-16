import psycopg2

table_name = 'airways'

conn = psycopg2.connect(user = "postgres",
                        password = "password",
                        host = "127.0.0.1",
                        port = "5432",
                        database = "airac_2021_08_simtoolkit")

with conn:
    cur = conn.cursor()

    sql_query = "DROP TABLE IF EXISTS " + table_name + ";"

    cur.execute(sql_query)
    conn.commit()

    # sql_query = "CREATE TABLE  " + table_name + "  (LIKE tbl_enroute_airways); " + \
    #             "ALTER TABLE " + table_name + \
    #             " ADD waypoint_identifier_2 varchar; " + \
    #             "ALTER TABLE " + table_name + \
    #             " ADD waypoint_latitude_2 double precision; " + \
    #             "ALTER TABLE " + table_name + \
    #             " ADD waypoint_longitude_2 double precision;" + \
    #             "ALTER TABLE " + table_name + \
    #             " ADD geom geometry;"

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

            #
            # if not (str(ats_route_id[k] == str(ats_route_id[k + 1]))):
            #     sub_route_id = 1
            # else:
            #     sub_route_id = sub_route_id + 1

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

    #
    #
    #
    #
    # while k < num_of_ids:
    #     if (inbound_distance[k+1] == 0):
    #         k = k + 1
    #     else:
    #         k = k + 1
    #
    #         if str(minimum_altitude_a[k]) == 'None':
    #             minimum_altitude_a_temp = 0
    #         else:
    #             minimum_altitude_a_temp = minimum_altitude_a[k]
    #
    #         if str(minimum_altitude_b[k]) == 'None':
    #             minimum_altitude_b_temp = 0
    #         else:
    #             minimum_altitude_b_temp = minimum_altitude_b[k]
    #
    #         if str(maximum_altitude[k]) == 'None':
    #             maximum_altitude_temp = 0
    #         else:
    #             maximum_altitude_temp = maximum_altitude[k]
    #
    #         if str(inbound_course[k]) == 'None':
    #             inbound_course_temp = -1
    #         else:
    #             inbound_course_temp = inbound_course[k]
    #
    #         if str(outbound_course[k]) == 'None':
    #             outbound_course_temp = -1
    #         else:
    #             outbound_course_temp = outbound_course[k]
    #
    #         sql_text = "INSERT INTO " + table_name + \
    #                    "(area_code," + \
    #                    "route_identifier," + \
    #                    "seqno," + \
    #                    "icao_code," + \
    #                    "waypoint_identifier," + \
    #                    "waypoint_latitude," + \
    #                    "waypoint_longitude," + \
    #                    "waypoint_description_code," + \
    #                    "route_type," + \
    #                    "flightlevel," + \
    #                    "direction_restriction," + \
    #                    "crusing_table_identifier," + \
    #                    "minimum_altitude_a," + \
    #                    "minimum_altitude_b," + \
    #                    "maximum_altitude," + \
    #                    "outbound_course," + \
    #                    "inbound_course," + \
    #                    "inbound_distance," + \
    #                    "waypoint_identifier_2," + \
    #                    "waypoint_latitude_2," + \
    #                    "waypoint_longitude_2," + \
    #                    "geom) " + \
    #                    "VALUES('" + \
    #                    str(area_code[k]) + "','" + \
    #                    str(route_identifier[k]) + "'," + \
    #                    str(seqno[k]) + ",'" + \
    #                    str(icao_code[k]) + "','" + \
    #                    str(waypoint_identifier[k]) + "'," + \
    #                    str(waypoint_latitude[k]) + "," + \
    #                    str(waypoint_longitude[k]) + ",'" + \
    #                    str(waypoint_description_code[k]) + "','" + \
    #                    str(route_type[k]) + "','" + \
    #                    str(flightlevel[k]) + "','" + \
    #                    str(direction_restriction[k]) + "','" + \
    #                    str(crusing_table_identifier[k]) + "'," + \
    #                    str(minimum_altitude_a_temp) + "," + \
    #                    str(minimum_altitude_b_temp) + "," + \
    #                    str(maximum_altitude_temp) + "," + \
    #                    str(outbound_course_temp) + "," + \
    #                    str(inbound_course_temp) + "," + \
    #                    str(inbound_distance[k]) + ",'" + \
    #                    str(waypoint_identifier[k + 1]) + "'," + \
    #                    str(waypoint_latitude[k + 1]) + "," + \
    #                    str(waypoint_longitude[k + 1]) + "," + \
    #                    "ST_LineFromText('LINESTRING(" + \
    #                    str(float(waypoint_longitude[k])) + " " + str(float(waypoint_latitude[k])) + "," + \
    #                    str(float(waypoint_longitude[k + 1])) + " " + str(float(waypoint_latitude[k + 1])) + ")',4326));"
    #         print(sql_text)
    #         cur.execute(sql_text)
    #         conn.commit()