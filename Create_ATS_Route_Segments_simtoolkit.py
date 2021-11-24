import psycopg2

from dbname_and_paths import db_name,airac

table_name = 'airways_segments_' + airac

conn = psycopg2.connect(user="postgres",
                        password="password",
                        host="127.0.0.1",
                        port="5432",
                        database=db_name)

with conn:
    cur = conn.cursor()

    sql_query = "DROP TABLE IF EXISTS " + table_name + ";"

    cur.execute(sql_query)
    conn.commit()

    sql_query = "CREATE TABLE  " + table_name + "  (LIKE tbl_enroute_airways); " + \
                "ALTER TABLE " + table_name + \
                " ADD waypoint_identifier_2 varchar; " + \
                "ALTER TABLE " + table_name + \
                " ADD waypoint_latitude_2 double precision; " + \
                "ALTER TABLE " + table_name + \
                " ADD waypoint_longitude_2 double precision;" + \
                "ALTER TABLE " + table_name + \
                " ADD geom geometry;"

    print(sql_query)

    cur.execute(sql_query)
    conn.commit()

    sql_query = "SELECT * " \
                "FROM public.tbl_enroute_airways "
        # query
    cur.execute(sql_query)

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

    # initialize parameters
    area_code = []
    route_identifier = []
    seqno = []
    icao_code = []
    waypoint_identifier = []
    waypoint_latitude = []
    waypoint_longitude = []
    waypoint_description_code = []
    route_type = []
    flightlevel = []
    direction_restriction = []
    crusing_table_identifier = []
    minimum_altitude1 = []
    minimum_altitude2 = []
    maximum_altitude = []
    outbound_course = []
    inbound_course = []
    inbound_distance = []

    # populate the parameters with the query results, row by row
    for row in results:
        area_code.append(row[0])
        route_identifier.append(row[1])
        seqno.append(row[2])
        icao_code.append(row[3])
        waypoint_identifier.append(row[4])
        waypoint_latitude.append(row[5])
        waypoint_longitude.append(row[6])
        waypoint_description_code.append(row[7])
        route_type.append(row[8])
        flightlevel.append(row[9])
        direction_restriction.append(row[10])
        crusing_table_identifier.append(row[11])
        minimum_altitude1.append(row[12])
        minimum_altitude2.append(row[13])
        maximum_altitude.append(row[14])
        outbound_course.append(row[15])
        inbound_course.append(row[16])
        inbound_distance.append(row[17])
    print("Number of rows: " + str(len(area_code)))
    num_of_ids = len(area_code)

    k = 0

    print(waypoint_identifier[k])
    print(waypoint_latitude[k])
    print(waypoint_longitude[k])

    if str(minimum_altitude1[k]) == 'None':
        minimum_altitude1_temp = 0
    else:
        minimum_altitude1_temp = minimum_altitude1[k]

    if str(minimum_altitude2[k]) == 'None':
        minimum_altitude2_temp = 0
    else:
        minimum_altitude2_temp = minimum_altitude2[k]

    if str(maximum_altitude[k]) == 'None':
        maximum_altitude_temp = 0
    else:
        maximum_altitude_temp = maximum_altitude[k]

    if str(inbound_course[k]) == 'None':
        inbound_course_temp = -1
    else:
        inbound_course_temp = inbound_course[k]

    if str(outbound_course[k]) == 'None':
        outbound_course_temp = -1
    else:
        outbound_course_temp = outbound_course[k]

    sql_text = "INSERT INTO " + table_name + \
               "(area_code," + \
               "route_identifier," + \
               "seqno," + \
               "icao_code," + \
               "waypoint_identifier," + \
               "waypoint_latitude," + \
               "waypoint_longitude," + \
               "waypoint_description_code," + \
               "route_type," + \
               "flightlevel," + \
               "direction_restriction," + \
               "crusing_table_identifier," + \
               "minimum_altitude1," + \
               "minimum_altitude2," + \
               "maximum_altitude," + \
               "outbound_course," + \
               "inbound_course," + \
               "inbound_distance," + \
               "waypoint_identifier_2," + \
               "waypoint_latitude_2," + \
               "waypoint_longitude_2," + \
               "geom) " + \
               "VALUES('" + \
               str(area_code[k]) + "','" + \
               str(route_identifier[k]) + "'," + \
               str(seqno[k]) + ",'" + \
               str(icao_code[k]) + "','" + \
               str(waypoint_identifier[k]) + "'," + \
               str(waypoint_latitude[k]) + "," + \
               str(waypoint_longitude[k]) + ",'" + \
               str(waypoint_description_code[k]) + "','" + \
               str(route_type[k]) + "','" + \
               str(flightlevel[k]) + "','" + \
               str(direction_restriction[k]) + "','" + \
               str(crusing_table_identifier[k]) + "'," + \
               str(minimum_altitude1_temp) + "," + \
               str(minimum_altitude2_temp) + "," + \
               str(maximum_altitude_temp) + "," + \
               str(outbound_course_temp) + "," + \
               str(inbound_course_temp) + "," + \
               str(inbound_distance[k]) + ",'" + \
               str(waypoint_identifier[k + 1]) + "'," + \
               str(waypoint_latitude[k + 1]) + "," + \
               str(waypoint_longitude[k + 1]) + "," + \
               "ST_LineFromText('LINESTRING(" + \
               str(float(waypoint_longitude[k])) + " " + str(float(waypoint_latitude[k])) + "," + \
               str(float(waypoint_longitude[k + 1])) + " " + str(float(waypoint_latitude[k + 1])) + ")',4326));"
    print(sql_text)

    cur.execute(sql_text)
    conn.commit()

    while k < num_of_ids - 1:
        if (inbound_distance[k + 1] == 0):
            k = k + 1
        else:
            k = k + 1

            if str(minimum_altitude1[k]) == 'None':
                minimum_altitude1_temp = 0
            else:
                minimum_altitude1_temp = minimum_altitude1[k]

            if str(minimum_altitude2[k]) == 'None':
                minimum_altitude2_temp = 0
            else:
                minimum_altitude2_temp = minimum_altitude2[k]

            if str(maximum_altitude[k]) == 'None':
                maximum_altitude_temp = 0
            else:
                maximum_altitude_temp = maximum_altitude[k]

            if str(inbound_course[k]) == 'None':
                inbound_course_temp = -1
            else:
                inbound_course_temp = inbound_course[k]

            if str(outbound_course[k]) == 'None':
                outbound_course_temp = -1
            else:
                outbound_course_temp = outbound_course[k]

            sql_text = "INSERT INTO " + table_name + \
                       "(area_code," + \
                       "route_identifier," + \
                       "seqno," + \
                       "icao_code," + \
                       "waypoint_identifier," + \
                       "waypoint_latitude," + \
                       "waypoint_longitude," + \
                       "waypoint_description_code," + \
                       "route_type," + \
                       "flightlevel," + \
                       "direction_restriction," + \
                       "crusing_table_identifier," + \
                       "minimum_altitude1," + \
                       "minimum_altitude2," + \
                       "maximum_altitude," + \
                       "outbound_course," + \
                       "inbound_course," + \
                       "inbound_distance," + \
                       "waypoint_identifier_2," + \
                       "waypoint_latitude_2," + \
                       "waypoint_longitude_2," + \
                       "geom) " + \
                       "VALUES('" + \
                       str(area_code[k]) + "','" + \
                       str(route_identifier[k]) + "'," + \
                       str(seqno[k]) + ",'" + \
                       str(icao_code[k]) + "','" + \
                       str(waypoint_identifier[k]) + "'," + \
                       str(waypoint_latitude[k]) + "," + \
                       str(waypoint_longitude[k]) + ",'" + \
                       str(waypoint_description_code[k]) + "','" + \
                       str(route_type[k]) + "','" + \
                       str(flightlevel[k]) + "','" + \
                       str(direction_restriction[k]) + "','" + \
                       str(crusing_table_identifier[k]) + "'," + \
                       str(minimum_altitude1_temp) + "," + \
                       str(minimum_altitude2_temp) + "," + \
                       str(maximum_altitude_temp) + "," + \
                       str(outbound_course_temp) + "," + \
                       str(inbound_course_temp) + "," + \
                       str(inbound_distance[k]) + ",'" + \
                       str(waypoint_identifier[k + 1]) + "'," + \
                       str(waypoint_latitude[k + 1]) + "," + \
                       str(waypoint_longitude[k + 1]) + "," + \
                       "ST_LineFromText('LINESTRING(" + \
                       str(float(waypoint_longitude[k])) + " " + str(float(waypoint_latitude[k])) + "," + \
                       str(float(waypoint_longitude[k + 1])) + " " + str(float(waypoint_latitude[k + 1])) + ")',4326));"
            cur.execute(sql_text)
            conn.commit()
            #print(sql_text)
            print("Airways Segments: "+str("{:.3f}".format((k / num_of_ids) * 100, 2)) + "% Completed")
