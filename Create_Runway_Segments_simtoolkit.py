import psycopg2

table_name = 'runway_segments_x'

conn = psycopg2.connect(user="postgres",
                        password="password",
                        host="127.0.0.1",
                        port="5432",
                        database="current_airac")

with conn:
    cur = conn.cursor()

    sql_query = "DROP TABLE IF EXISTS " + table_name + ";"

    cur.execute(sql_query)
    conn.commit()

    sql_query = "CREATE TABLE  " + table_name + "  (LIKE tbl_runways); " + \
                "ALTER TABLE " + table_name + \
                " ADD geom geometry;"

    print(sql_query)

    cur.execute(sql_query)
    conn.commit()

    sql_query = "select distinct airport_identifier " \
                "from " \
                "(SELECT airport_identifier,count(*) " \
                "FROM public.tbl_runways " \
                "group by airport_identifier) a " \
                "where count >= 2 " \
                "order by airport_identifier desc " \
 \
    # query
    cur.execute(sql_query)

    airport_identifier_list = []

    weird_airport_list = []

    results = cur.fetchall()

    for row in results:
        airport_identifier_list.append(row[0])

    print(airport_identifier_list)

    print("Total number of airports is:  ", len(results))

    total = len(results)

    k = 0

    for airport_id in airport_identifier_list:  # each airport identifier
        print(str("{:.3f}".format((k / total) * 100, 2)) + "% Completed")
        print(airport_id)

        k = k + 1

        sql_query = "SELECT runway_identifier " \
                    "FROM public.tbl_runways " \
                    "where airport_identifier like '" + airport_id + "' "

        cur.execute(sql_query)

        runway_identifier_list = []

        results = cur.fetchall()

        for row in results:
            runway_identifier_list.append(row[0])

        for runway_id_1 in runway_identifier_list:  # each runway identifier

            sql_query = "SELECT * " \
                        "FROM public.tbl_runways " \
                        "where airport_identifier like '" + airport_id + "'  " \
                        "and runway_identifier like '" + runway_id_1 + "'; "
            # query
            cur.execute(sql_query)

            results = cur.fetchone()


            # initialize parameters
            area_code = results[0]
            icao_code = results[1]
            airport_identifier = results[2]
            runway_identifier = results[3]
            runway_latitude = results[4]
            runway_longitude = results[5]
            runway_gradient = results[6]
            runway_magnetic_bearing = results[7]
            runway_true_bearing = results[8]
            landing_threshold_elevation = results[9]
            displaced_threshold_distance = results[10]
            threshold_crossing_height = results[11]
            runway_length = results[12]
            runway_width = results[13]
            llz_identifier = results[14]
            llz_mls_gls_category = results[15]

            # # populate the parameters with the query results, row by row
            # for row in results:
            #     area_code.append(row[0])
            #     icao_code.append(row[1])
            #     airport_identifier.append(row[2])
            #     runway_identifier.append(row[3])
            #     runway_latitude.append(row[4])
            #     runway_longitude.append(row[5])
            #     runway_gradient.append(row[6])
            #     runway_magnetic_bearing.append(row[7])
            #     runway_true_bearing.append(row[8])
            #     landing_threshold_elevation.append(row[9])
            #     displaced_threshold_distance.append(row[10])
            #     threshold_crossing_height.append(row[11])
            #     runway_length.append(row[12])
            #     runway_width.append(row[13])
            #     llz_identifier.append(row[14])
            #     llz_mls_gls_category.append(row[15])

            if str(runway_gradient) == 'None':
                runway_gradient_temp = -1
            else:
                runway_gradient_temp = runway_gradient

            if str(llz_identifier) == 'None':
                llz_identifier_temp = "None"
            else:
                llz_identifier_temp = llz_identifier

            if str(llz_mls_gls_category) == 'None':
                llz_mls_gls_category_temp = "None"
            else:
                llz_mls_gls_category_temp = llz_mls_gls_category

            runway_a = runway_identifier

            sql_text_2 = "INSERT INTO " + table_name + \
                         "(area_code," + \
                         "icao_code," + \
                         "airport_identifier," + \
                         "runway_identifier," + \
                         "runway_latitude," + \
                         "runway_longitude," + \
                         "runway_gradient," + \
                         "runway_magnetic_bearing," + \
                         "runway_true_bearing," + \
                         "landing_threshold_elevation," + \
                         "displaced_threshold_distance," + \
                         "threshold_crossing_height," + \
                         "runway_length," + \
                         "runway_width," + \
                         "llz_identifier," + \
                         "llz_mls_gls_category ," + \
                         "geom) " + \
                         "VALUES('" + \
                         str(area_code) + "','" + \
                         str(icao_code) + "','" + \
                         str(airport_identifier) + "','" + \
                         str(runway_identifier) + "'," + \
                         str(runway_latitude) + "," + \
                         str(runway_longitude) + "," + \
                         str(runway_gradient_temp) + "," + \
                         str(runway_magnetic_bearing) + "," + \
                         str(runway_true_bearing) + "," + \
                         str(landing_threshold_elevation) + "," + \
                         str(displaced_threshold_distance) + "," + \
                         str(threshold_crossing_height) + "," + \
                         str(runway_length) + "," + \
                         str(runway_width) + ",'" + \
                         str(llz_identifier_temp) + "','" + \
                         str(llz_mls_gls_category_temp) + "'," + \
                         "ST_LineFromText('LINESTRING(" + \
                         str(float(runway_longitude)) + " " + str(float(runway_latitude)) + ","

            rwy_dir_1 = int(runway_id_1[2:4])

            if rwy_dir_1 < 18:
                rwy_dir_2 = rwy_dir_1 + 18
            else:
                rwy_dir_2 = rwy_dir_1 - 18

            if rwy_dir_2 < 10:
                if rwy_dir_2 == 0:
                    runway_id_2 = 'RW36'
                else:
                    runway_id_2 = 'RW0' + str(rwy_dir_2)
            else:
                runway_id_2 = 'RW' + str(rwy_dir_2)

            if len(runway_id_1) >= 5:
                if runway_id_1[4] == 'L':
                    runway_id_2 = runway_id_2 + 'R'
                elif runway_id_1[4] == 'R':
                    runway_id_2 = runway_id_2 + 'L'
                elif runway_id_1[4] == 'T':
                    runway_id_2 = runway_id_2 + 'T'
                else:
                    runway_id_2 = runway_id_2 + 'C'

            if len(runway_id_1) == 6:
                runway_id_2 = runway_id_2 + 'T'


            sql_query = "SELECT runway_latitude, runway_longitude " \
                        "FROM public.tbl_runways " \
                        "where airport_identifier like '" + airport_id + "'  " \
                        "and runway_identifier like '" + runway_id_2 + "'; "

            cur.execute(sql_query)

            results = cur.fetchmany()

            #print(results)

            # initialize parameters

            # # populate the parameters with the query results, row by row
            # for row in results:
            #     runway_latitude_2.append(row[0])
            #     runway_longitude_2.append(row[1])

            if not (len(results) == 0):
                runway_latitude_2 = results[0][0]
                runway_longitude_2 = results[0][1]
                sql_text_2 = sql_text_2 + str(float(runway_longitude_2)) + " " + str(
                    float(runway_latitude_2)) + ")',4326));"
            else:
                sql_text_2 = sql_text_2 + str(float(runway_longitude)) + " " + str(
                    float(runway_latitude)) + ")',4326));"
                weird_airport_list.append(airport_identifier)

            cur.execute(sql_text_2)
            conn.commit()
    print(str("{:.3f}".format((k / total) * 100, 2)) + "% Completed")
    print(weird_airport_list)
