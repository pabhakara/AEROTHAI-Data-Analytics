def create_runway_segments(db_name,schema_name):

    import psycopg2
    from pyproj import Transformer
    import math

    #from autoprocess_simtoolkit import db_name, path_db, schema_name

    def convert_wgs_to_utm(lon: float, lat: float):
        """Based on lat and lng, return best utm epsg-code"""
        utm_band = str((math.floor((lon + 180) / 6) % 60) + 1)
        if len(utm_band) == 1:
            utm_band = '0' + utm_band
        if lat >= 0:
            epsg_code = '326' + utm_band
            return epsg_code
        epsg_code = '327' + utm_band
        return epsg_code

    table_name = 'runway_segments'

    conn = psycopg2.connect(
        user='postgres', password='password',
        host='127.0.0.1', port='5432',
        database=db_name,
        options="-c search_path=dbo," + schema_name
    )

    with conn:
        cur = conn.cursor()

        sql_query = "DROP TABLE IF EXISTS " + table_name + ";"

        cur.execute(sql_query)
        conn.commit()

        sql_query = "CREATE TABLE  " + table_name + "  (LIKE tbl_runways); " + \
                    "ALTER TABLE " + table_name + \
                    " ADD geom geometry;"

        #print(sql_query)

        cur.execute(sql_query)
        conn.commit()

        sql_query = "select distinct airport_identifier " \
                    "from " \
                    "(SELECT airport_identifier,count(*) " \
                    "FROM tbl_runways " \
                    "group by airport_identifier) a " \
                    "where count >= 2 " \
                    "order by airport_identifier desc " \

        # query
        cur.execute(sql_query)

        airport_identifier_list = []

        weird_airport_list = []

        results = cur.fetchall()

        for row in results:
            airport_identifier_list.append(row[0])

        print("Total number of airports is:  ", len(results))

        total = len(results)

        k = 0

        for airport_id in airport_identifier_list:  # each airport identifier
            print("Runway Segments: " + str("{:.3f}".format((k / total) * 100, 2)) + "% Completed")

            k = k + 1

            sql_query = "SELECT runway_identifier " \
                        "FROM tbl_runways " \
                        "where airport_identifier like '" + airport_id + "' "

            cur.execute(sql_query)

            runway_identifier_list = []

            results = cur.fetchall()

            for row in results:
                runway_identifier_list.append(row[0])

            for runway_id_1 in runway_identifier_list:  # each runway identifier

                sql_query = "SELECT * " \
                            "FROM tbl_runways " \
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
                            "FROM tbl_runways " \
                            "where airport_identifier like '" + airport_id + "'  " \
                            "and runway_identifier like '" + runway_id_2 + "'; "

                cur.execute(sql_query)

                results = cur.fetchmany()

                if not (len(results) == 0):
                    runway_latitude_2 = results[0][0]
                    runway_longitude_2 = results[0][1]
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
                             str(float(runway_longitude)) + " " + str(float(runway_latitude)) + "," + \
                             str(float(runway_longitude_2)) + " " + str(
                             float(runway_latitude_2)) + ")',4326));"
                    cur.execute(sql_text_2)
                    conn.commit()
                else:
                    UTM_zone = convert_wgs_to_utm(runway_longitude, runway_latitude)

                    transformer = Transformer.from_crs("epsg:4326", "epsg:" + str(UTM_zone))

                    runway_1_xy = transformer.transform(runway_latitude, runway_longitude)

                    #print(runway_1_xy)

                    runway_length_m = runway_length / 3.048

                    if 0 < runway_true_bearing < 90:
                        runway_2_x = runway_1_xy[0] + runway_length_m * math.cos(runway_true_bearing * math.pi/180)
                        runway_2_y = runway_1_xy[1] + runway_length_m * math.sin(runway_true_bearing * math.pi/180)
                    elif 90 < runway_true_bearing < 180:
                        runway_2_x = runway_1_xy[0] + runway_length_m * math.cos((runway_true_bearing - 90) * math.pi / 180)
                        runway_2_y = runway_1_xy[1] - runway_length_m * math.sin((runway_true_bearing - 90) * math.pi / 180)
                    elif 180 < runway_true_bearing < 270:
                        runway_2_x = runway_1_xy[0] - runway_length_m * math.cos((runway_true_bearing - 180) * math.pi/180)
                        runway_2_y = runway_1_xy[1] - runway_length_m * math.sin((runway_true_bearing - 180) * math.pi/180)
                    else:
                        runway_2_x = runway_1_xy[0] - runway_length_m * math.cos((runway_true_bearing - 270) * math.pi / 180)
                        runway_2_y = runway_1_xy[1] + runway_length_m * math.sin((runway_true_bearing - 270) * math.pi / 180)

                    #print(runway_1_xy)
                    #print([runway_2_x,runway_2_y])

                    #print([runway_2_x - runway_1_xy[0] , runway_2_y - runway_1_xy[1]])

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
                                 "ST_Transform(ST_LineFromText('LINESTRING(" + \
                                 str(runway_1_xy[0]) + " " + str(runway_1_xy[1]) + "," + \
                                 str(runway_2_x) + " " + str(runway_2_y) + ")'," + UTM_zone + "),4326));"
                    weird_airport_list.append(airport_identifier)
                    #print(sql_text_2)
                    cur.execute(sql_text_2)
                    conn.commit()
        #print("Runway Segments: " + str("{:.3f}".format((k / total) * 100, 2)) + "% Completed")
        #print(weird_airport_list)
