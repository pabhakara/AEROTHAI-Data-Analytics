def create_holding_legs_from_iaps(db_name,schema_name):
    import psycopg2.extras
    import psycopg2
    from functools import partial
    from pyproj import Proj, transform
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


    # Try to connect to the local PostGresSQL database in which we will store our flight trajectories coupled with FPL data.
    conn_postgres = psycopg2.connect(
        user='postgres', password='password',
        host='127.0.0.1', port='5432',
        database=db_name,
        options="-c search_path=dbo," + schema_name
    )
    with conn_postgres:
        cursor_postgres = conn_postgres.cursor(cursor_factory=psycopg2.extras.DictCursor)

        table_name = 'holding_iap_legs'
        table_name2 = table_name + '_geom'

        postgres_sql_text = "DROP TABLE IF EXISTS " + table_name + "; \n" + \
                            "CREATE TABLE " + table_name + " " + \
                            "(LIKE holdings" + ")" + \
                            "WITH (OIDS=FALSE); \n" + \
                            "ALTER TABLE " + table_name + " " \
                            "OWNER TO postgres;"

        cursor_postgres.execute(postgres_sql_text)
        conn_postgres.commit()

        postgres_sql_text = "ALTER TABLE " + table_name + " \n" + \
                            "RENAME COLUMN region_code to airport_identifier;"

        cursor_postgres.execute(postgres_sql_text)
        conn_postgres.commit()

        postgres_sql_text = "ALTER TABLE " + table_name + " \n" + \
                            "RENAME COLUMN icao_code to procedure_identifier;"

        cursor_postgres.execute(postgres_sql_text)
        conn_postgres.commit()

        postgres_sql_text = "ALTER TABLE " + table_name + " \n" + \
                            "RENAME COLUMN holding_name to waypoint_description_code;"

        cursor_postgres.execute(postgres_sql_text)
        conn_postgres.commit()

        postgres_sql_text = " SELECT * FROM tbl_iaps " + \
                            " where airport_identifier like '%'  " + \
                            " and (path_termination like 'H%') "

        #postgres_sql_text = "SELECT * from tbl_holdings WHERE icao_code like 'VT%'"

        #print(postgres_sql_text)

        cursor_postgres.execute(postgres_sql_text)

        record = cursor_postgres.fetchall()

        num_of_records = len(record)
        print("num_of_record: ", num_of_records)

        cursor_postgres = conn_postgres.cursor()

        k = 0

        while k < num_of_records - 1:
            temp_1 = record[k]

            area_code = str(temp_1['area_code'])
            airport_identifier = str(temp_1['airport_identifier'])
            procedure_identifier = str(temp_1['procedure_identifier'])
            waypoint_identifier = str(temp_1['waypoint_identifier'])
            waypoint_description_code = str(temp_1['waypoint_description_code'])
            waypoint_latitude = str(temp_1['waypoint_latitude'])
            waypoint_longitude = str(temp_1['waypoint_longitude'])
            duplicate_identifier = "0"
            inbound_holding_course = str(temp_1['magnetic_course'])
            turn_direction = str(temp_1['turn_direction'])

            route_distance_holding_distance_time = str(temp_1['route_distance_holding_distance_time'])
            distance_time = str(temp_1['distance_time'])

            if distance_time == "T":
                leg_time = route_distance_holding_distance_time
                leg_length = '0'
            else:
                leg_length = route_distance_holding_distance_time
                leg_time = '0'

            minimum_altitude = str(temp_1['altitude1'])
            if minimum_altitude == 'None':
                minimum_altitude = '0'
            maximum_altitude = str(temp_1['altitude2'])
            if maximum_altitude == 'None':
                maximum_altitude = '99999'
            holding_speed = str(temp_1['speed_limit'])
            if holding_speed == 'None':
                holding_speed = '999'

            UTM_zone = convert_wgs_to_utm(temp_1['waypoint_longitude'], temp_1['waypoint_latitude'])

            transformer = Transformer.from_crs("epsg:4326", "epsg:" + str(UTM_zone))

            postgres_sql_text = "INSERT INTO \"" + table_name + "\" " + \
                                "(\"area_code\"," + \
                                "\"airport_identifier\"," + \
                                "\"procedure_identifier\"," + \
                                "\"waypoint_identifier\"," + \
                                "\"waypoint_description_code\"," + \
                                "\"waypoint_latitude\"," + \
                                "\"waypoint_longitude\"," + \
                                "\"duplicate_identifier\"," + \
                                "\"inbound_holding_course\"," + \
                                "\"turn_direction\"," + \
                                "\"leg_length\"," + \
                                "\"leg_time\"," + \
                                "\"minimum_altitude\"," + \
                                "\"maximum_altitude\"," + \
                                "\"holding_speed\"," + \
                                "\"geom\")"

            waypoint_xy = transformer.transform(temp_1['waypoint_latitude'], temp_1['waypoint_longitude'])

            postgres_sql_text = postgres_sql_text + " VALUES('" + area_code + "','" \
                                + airport_identifier + "','" \
                                + procedure_identifier + "','" \
                                + waypoint_identifier + "','" \
                                + waypoint_description_code + "'," \
                                + waypoint_latitude + "," \
                                + waypoint_longitude + "," \
                                + duplicate_identifier + "," \
                                + inbound_holding_course + ",'" \
                                + turn_direction + "'," \
                                + leg_length + "," \
                                + leg_time + "," \
                                + minimum_altitude + "," \
                                + maximum_altitude + "," \
                                + holding_speed + "," \
                                + "ST_Transform(ST_SetSRID(ST_GeomFromEWKT('CIRCULARSTRING("


            course_deg = (temp_1['magnetic_course'])

            turn = str(temp_1['turn_direction'])
            length = leg_length
            time = leg_time

            diameter = 1852 * 2
            distance = 1852 * 3

            #print(course_deg)

            if course_deg >= 270 or course_deg <= 90:
                north_bound = True
            else:
                north_bound = False

            if course_deg < 180:
                east_bound = True
            else:
                east_bound = False

            course_rad = course_deg / 180 * math.pi

            # For CIRCULARSTRING, a holding pattern is defined by 8 waypoints.
            if turn == "R":
                if north_bound and (east_bound):  # NE
                    point_1 = transformer.transform(temp_1['waypoint_latitude'], temp_1['waypoint_longitude'])
                    point_3 = (point_1[0] + diameter * math.cos(course_rad), point_1[1] - diameter * math.sin(course_rad))
                    point_5 = (point_3[0] - distance * math.cos(math.pi / 2 - course_rad),
                               point_3[1] - distance * math.sin(math.pi / 2 - course_rad))
                    point_7 = (point_5[0] - diameter * math.cos(course_rad), point_5[1] + diameter * math.sin(course_rad))

                    point_2 = ((point_1[0] + point_3[0]) / 2 + diameter * math.cos(course_rad) / 2
                               , (point_1[1] + point_3[1]) / 2 + diameter * math.sin(course_rad) / 2)
                    point_6 = ((point_5[0] + point_7[0]) / 2 - diameter * math.cos(course_rad) / 2,
                               (point_5[1] + point_7[1]) / 2 - diameter * math.sin(course_rad) / 2)

                elif not (north_bound) and not (east_bound):  # SW
                    point_1 = transformer.transform(temp_1['waypoint_latitude'], temp_1['waypoint_longitude'])
                    point_3 = (point_1[0] + diameter * math.cos(course_rad), point_1[1] - diameter * math.sin(course_rad))
                    point_5 = (point_3[0] - distance * math.cos(math.pi / 2 - course_rad),
                               point_3[1] - distance * math.sin(math.pi / 2 - course_rad))
                    point_7 = (point_5[0] - diameter * math.cos(course_rad), point_5[1] + diameter * math.sin(course_rad))

                    point_2 = ((point_1[0] + point_3[0]) / 2 + diameter * math.cos(course_rad) / 2
                               , (point_1[1] + point_3[1]) / 2 + diameter * math.sin(course_rad) / 2)
                    point_6 = ((point_5[0] + point_7[0]) / 2 - diameter * math.cos(course_rad) / 2,
                               (point_5[1] + point_7[1]) / 2 - diameter * math.sin(course_rad) / 2)

                elif not (north_bound) and (east_bound):  # SE
                    point_1 = transformer.transform(temp_1['waypoint_latitude'], temp_1['waypoint_longitude'])
                    point_3 = (point_1[0] + diameter * math.cos(course_rad), point_1[1] - diameter * math.sin(course_rad))
                    point_5 = (point_3[0] - distance * math.cos(math.pi / 2 - course_rad),
                               point_3[1] - distance * math.sin(math.pi / 2 - course_rad))
                    point_7 = (point_5[0] - diameter * math.cos(course_rad), point_5[1] + diameter * math.sin(course_rad))

                    point_2 = ((point_1[0] + point_3[0]) / 2 - diameter * math.cos(course_rad) / 2
                               , (point_1[1] + point_3[1]) / 2 - diameter * math.sin(course_rad) / 2)
                    point_6 = ((point_5[0] + point_7[0]) / 2 + diameter * math.cos(course_rad) / 2,
                               (point_5[1] + point_7[1]) / 2 + diameter * math.sin(course_rad) / 2)
                else:  # NW
                    point_1 = transformer.transform(temp_1['waypoint_latitude'], temp_1['waypoint_longitude'])
                    point_3 = (point_1[0] + diameter * math.cos(course_rad), point_1[1] - diameter * math.sin(course_rad))
                    point_5 = (point_3[0] - distance * math.cos(math.pi / 2 - course_rad),
                               point_3[1] - distance * math.sin(math.pi / 2 - course_rad))
                    point_7 = (point_5[0] - diameter * math.cos(course_rad), point_5[1] + diameter * math.sin(course_rad))

                    point_2 = ((point_1[0] + point_3[0]) / 2 - diameter * math.cos(course_rad) / 2
                               , (point_1[1] + point_3[1]) / 2 - diameter * math.sin(course_rad) / 2)
                    point_6 = ((point_5[0] + point_7[0]) / 2 + diameter * math.cos(course_rad) / 2,
                               (point_5[1] + point_7[1]) / 2 + diameter * math.sin(course_rad) / 2)

            else:  # turn == "L" :
                if north_bound and (east_bound):  # NE
                    point_1 = transformer.transform(temp_1['waypoint_latitude'], temp_1['waypoint_longitude'])
                    point_3 = (point_1[0] - diameter * math.cos(course_rad), point_1[1] + diameter * math.sin(course_rad))
                    point_5 = (point_3[0] - distance * math.cos(math.pi / 2 - course_rad),
                               point_3[1] - distance * math.sin(math.pi / 2 - course_rad))
                    point_7 = (point_5[0] + diameter * math.cos(course_rad), point_5[1] - diameter * math.sin(course_rad))

                    point_2 = ((point_1[0] + point_3[0]) / 2 + diameter * math.cos(course_rad) / 2
                               , (point_1[1] + point_3[1]) / 2 + diameter * math.sin(course_rad) / 2)
                    point_6 = ((point_5[0] + point_7[0]) / 2 - diameter * math.cos(course_rad) / 2,
                               (point_5[1] + point_7[1]) / 2 - diameter * math.sin(course_rad) / 2)

                elif not (north_bound) and not (east_bound):  # SW
                    point_1 = transformer.transform(temp_1['waypoint_latitude'], temp_1['waypoint_longitude'])
                    point_3 = (point_1[0] - diameter * math.cos(course_rad), point_1[1] + diameter * math.sin(course_rad))
                    point_5 = (point_3[0] - distance * math.cos(math.pi / 2 - course_rad),
                               point_3[1] - distance * math.sin(math.pi / 2 - course_rad))
                    point_7 = (point_5[0] + diameter * math.cos(course_rad), point_5[1] - diameter * math.sin(course_rad))

                    point_2 = ((point_1[0] + point_3[0]) / 2 + diameter * math.cos(course_rad) / 2
                               , (point_1[1] + point_3[1]) / 2 + diameter * math.sin(course_rad) / 2)
                    point_6 = ((point_5[0] + point_7[0]) / 2 - diameter * math.cos(course_rad) / 2,
                               (point_5[1] + point_7[1]) / 2 - diameter * math.sin(course_rad) / 2)

                elif not (north_bound) and (east_bound):  # SE
                    point_1 = transformer.transform(temp_1['waypoint_latitude'], temp_1['waypoint_longitude'])
                    point_3 = (point_1[0] - diameter * math.cos(course_rad), point_1[1] + diameter * math.sin(course_rad))
                    point_5 = (point_3[0] - distance * math.cos(math.pi / 2 - course_rad),
                               point_3[1] - distance * math.sin(math.pi / 2 - course_rad))
                    point_7 = (point_5[0] + diameter * math.cos(course_rad), point_5[1] - diameter * math.sin(course_rad))

                    point_2 = ((point_1[0] + point_3[0]) / 2 - diameter * math.cos(course_rad) / 2
                               , (point_1[1] + point_3[1]) / 2 - diameter * math.sin(course_rad) / 2)
                    point_6 = ((point_5[0] + point_7[0]) / 2 + diameter * math.cos(course_rad) / 2,
                               (point_5[1] + point_7[1]) / 2 + diameter * math.sin(course_rad) / 2)
                else:  # NW
                    point_1 = transformer.transform(temp_1['waypoint_latitude'], temp_1['waypoint_longitude'])
                    point_3 = (point_1[0] - diameter * math.cos(course_rad), point_1[1] + diameter * math.sin(course_rad))
                    point_5 = (point_3[0] - distance * math.cos(math.pi / 2 - course_rad),
                               point_3[1] - distance * math.sin(math.pi / 2 - course_rad))
                    point_7 = (point_5[0] + diameter * math.cos(course_rad), point_5[1] - diameter * math.sin(course_rad))

                    point_2 = ((point_1[0] + point_3[0]) / 2 - diameter * math.cos(course_rad) / 2
                               , (point_1[1] + point_3[1]) / 2 - diameter * math.sin(course_rad) / 2)
                    point_6 = ((point_5[0] + point_7[0]) / 2 + diameter * math.cos(course_rad) / 2,
                               (point_5[1] + point_7[1]) / 2 + diameter * math.sin(course_rad) / 2)
            if course_deg == 360:
                point_2 = ((point_1[0] + point_3[0]) / 2
                           , (point_1[1] + point_3[1]) / 2 + diameter / 2)
                point_6 = ((point_5[0] + point_7[0]) / 2,
                           (point_5[1] + point_7[1]) / 2 - diameter / 2)
            if course_deg == 270:
                point_2 = ((point_1[0] + point_3[0]) / 2 - diameter / 2
                           , (point_1[1] + point_3[1]) / 2)
                point_6 = ((point_5[0] + point_7[0]) / 2 + diameter / 2,
                           (point_5[1] + point_7[1]) / 2)
            if course_deg == 90:
                point_2 = ((point_1[0] + point_3[0]) / 2 + diameter / 2
                           , (point_1[1] + point_3[1]) / 2)
                point_6 = ((point_5[0] + point_7[0]) / 2 - diameter / 2,
                           (point_5[1] + point_7[1]) / 2)
            if course_deg == 180:
                point_2 = ((point_1[0] + point_3[0]) / 2
                           , (point_1[1] + point_3[1]) / 2 - diameter / 2)
                point_6 = ((point_5[0] + point_7[0]) / 2,
                           (point_5[1] + point_7[1]) / 2 + diameter / 2)

            point_4 = ((point_3[0] + point_5[0]) / 2, (point_3[1] + point_5[1]) / 2)
            point_8 = ((point_7[0] + point_1[0]) / 2, (point_7[1] + point_1[1]) / 2)

            # postgres_sql_text2 = "INSERT INTO \"" + table_name2 + "\" " + \
            #                      "(\"waypoint_name\"," + \
            #                      "\"geom\") " + \
            #                      " VALUES('point_3_" + str(temp_1['waypoint_identifier']) + "'," \
            #                      + "ST_Transform(ST_SetSRID(ST_MakePoint(" \
            #                      + str(point_3[0]) + "," + str(point_3[1]) + ")," \
            #                      + str(UTM_zone) + "), 4326));"
            # print(postgres_sql_text2)
            #
            # cursor_postgres.execute(postgres_sql_text2)
            #
            # conn_postgres.commit()

            postgres_sql_text = postgres_sql_text + \
                                str(point_1[0]) + " " + str(point_1[1]) + "," + \
                                str(point_2[0]) + " " + str(point_2[1]) + "," + \
                                str(point_3[0]) + " " + str(point_3[1]) + "," + \
                                str(point_4[0]) + " " + str(point_4[1]) + "," + \
                                str(point_5[0]) + " " + str(point_5[1]) + "," + \
                                str(point_6[0]) + " " + str(point_6[1]) + "," + \
                                str(point_7[0]) + " " + str(point_7[1]) + "," + \
                                str(point_8[0]) + " " + str(point_8[1]) + "," + \
                                str(point_1[0]) + " " + str(point_1[1]) + ")'), " + \
                                str(UTM_zone) + "), 4326)); "

            #print(postgres_sql_text)

            cursor_postgres.execute(postgres_sql_text)

            conn_postgres.commit()
            #print("Holding Legs from IAP: " + str("{:.3f}".format((k / num_of_records) * 100, 2)) + "% Completed")

            k = k + 1
            temp_1 = record[k]
