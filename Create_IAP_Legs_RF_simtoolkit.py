def create_iap_legs_rf(db_name,schema_name):
    import psycopg2.extras
    import psycopg2
    from pyproj import Transformer
    import math


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

        table_name = 'iap_legs_rf'

        postgres_sql_text = "DROP TABLE IF EXISTS " + table_name + "; \n" + \
                            "CREATE TABLE " + table_name + " " + \
                            "(area_code character varying, " + \
                            "airport_identifier character varying, " + \
                            "procedure_identifier character varying, " + \
                            "transition_identifier character varying, " + \
                            "geom geometry)" + \
                            "WITH (OIDS=FALSE); \n" + \
                            "ALTER TABLE " + table_name + " " \
                                                          "OWNER TO postgres;"

        print(postgres_sql_text)

        cursor_postgres.execute(postgres_sql_text)

        conn_postgres.commit()

        postgres_sql_text = "select * " \
                            "from public.tbl_iaps " \
                            "where concat(airport_identifier,procedure_identifier,transition_identifier) in " \
                            "(SELECT distinct concat(airport_identifier,procedure_identifier,transition_identifier) from " \
                            "public.tbl_iaps " + \
                            "WHERE path_termination = 'RF') " \
                            "and not(waypoint_identifier is null)" \
                            "order by airport_identifier, procedure_identifier,route_type, transition_identifier, seqno " \

        print(postgres_sql_text)

        cursor_postgres.execute(postgres_sql_text)

        record = cursor_postgres.fetchall()

        num_of_records = len(record)
        print("num_of_record: ", num_of_records)

        cursor_postgres = conn_postgres.cursor()

        k = 0

        temp_1 = record[k]
        temp_2 = record[k + 1]

        area_code = str(temp_1['area_code'])
        airport_identifier = str(temp_1['airport_identifier'])
        procedure_identifier = str(temp_1['procedure_identifier'])
        transition_identifier = str(temp_1['transition_identifier'])

        UTM_zone = convert_wgs_to_utm(temp_1['waypoint_longitude'], temp_1['waypoint_latitude'])

        transformer = Transformer.from_crs("epsg:4326", "epsg:" + str(UTM_zone))

        postgres_sql_text = "INSERT INTO \"" + table_name + "\" " + \
                            "(\"area_code\"," + \
                            "\"airport_identifier\"," + \
                            "\"procedure_identifier\"," + \
                            "\"transition_identifier\"," + \
                            "\"geom\")"

        waypoint_xy = transformer.transform(temp_1['waypoint_latitude'], temp_1['waypoint_longitude'])

        postgres_sql_text = postgres_sql_text + " VALUES('" + area_code + "','" \
                            + airport_identifier + "','" \
                            + procedure_identifier + "','" \
                            + transition_identifier + "'," \
                            + "ST_Transform(ST_SetSRID(ST_GeomFromEWKT('CIRCULARSTRING(" \
                            + str(waypoint_xy[0]) + " " + str(waypoint_xy[1]) + ","

        while k < num_of_records - 1:
            while (temp_1['procedure_identifier'] == temp_2['procedure_identifier']) and \
                    (temp_1['transition_identifier'] == temp_2['transition_identifier']) and \
                    (temp_1['path_termination'] == 'TF' or
                     temp_1['path_termination'] == 'DF' or
                     temp_1['path_termination'] == 'CF' or
                     temp_1['path_termination'] == 'FD' or
                     temp_1['path_termination'] == 'FC' or
                     temp_1['path_termination'] == 'RF' or
                     temp_1['path_termination'] == 'IF'):
                if temp_2['path_termination'] == 'RF' and not(str(temp_2['center_waypoint_latitude']) == 'None'):
                    arc_center_latlong = (temp_2['center_waypoint_latitude'], temp_2['center_waypoint_longitude'])
                    start_wp_latlong = (temp_1['waypoint_latitude'], temp_1['waypoint_longitude'])
                    end_wp_latlong = (temp_2['waypoint_latitude'], temp_2['waypoint_longitude'])

                    start_wp_xy = transformer.transform(start_wp_latlong[0], start_wp_latlong[1])
                    end_wp_xy = transformer.transform(end_wp_latlong[0], end_wp_latlong[1])
                    arc_center_xy = transformer.transform(arc_center_latlong[0], arc_center_latlong[1])

                    mid_wp_xy = ((start_wp_xy[0] + end_wp_xy[0]) / 2, (start_wp_xy[1] + end_wp_xy[1]) / 2)

                    arc_radius = math.sqrt(
                        (start_wp_xy[0] - arc_center_xy[0]) ** 2 + (start_wp_xy[1] - arc_center_xy[1]) ** 2)

                    theta = math.atan((mid_wp_xy[1] - arc_center_xy[1]) / (mid_wp_xy[0] - arc_center_xy[0]))

                    if ((end_wp_xy[0] > start_wp_xy[0]) and
                        (end_wp_xy[1] < start_wp_xy[1]) and
                        str(temp_2['turn_direction']) == 'L') or \
                            ((end_wp_xy[0] > start_wp_xy[0]) and
                             (end_wp_xy[1] > start_wp_xy[1]) and
                             str(temp_2['turn_direction']) == 'R') or \
                            ((end_wp_xy[0] < start_wp_xy[0]) and
                             (end_wp_xy[1] < start_wp_xy[1]) and
                             str(temp_2['turn_direction']) == 'L') or \
                            ((end_wp_xy[0] < start_wp_xy[0]) and
                             (end_wp_xy[1] > start_wp_xy[1]) and
                             str(temp_2['turn_direction']) == 'R'):
                        x_comp = -math.cos(theta) * arc_radius + arc_center_xy[0]
                        y_comp = -math.sin(theta) * arc_radius + arc_center_xy[1]
                    else:
                        x_comp = math.cos(theta) * arc_radius + arc_center_xy[0]
                        y_comp = math.sin(theta) * arc_radius + arc_center_xy[1]

                    postgres_sql_text = postgres_sql_text + \
                                        str(start_wp_xy[0]) + " " + str(start_wp_xy[1]) + "," + \
                                        str(start_wp_xy[0]) + " " + str(start_wp_xy[1]) + "," + \
                                        str(x_comp) + " " + str(y_comp) + "," + \
                                        str(end_wp_xy[0]) + " " + str(end_wp_xy[1]) + "," + \
                                        str(end_wp_xy[0]) + " " + str(end_wp_xy[1]) + "," + \
                                        str(end_wp_xy[0]) + " " + str(end_wp_xy[1]) + ","

                else:
                    waypoint_xy = transformer.transform(temp_1['waypoint_latitude'], temp_1['waypoint_longitude'])

                    postgres_sql_text = postgres_sql_text + \
                                        str(waypoint_xy[0]) + " " + str(waypoint_xy[1]) + ","
                    postgres_sql_text = postgres_sql_text + \
                                        str(waypoint_xy[0]) + " " + str(waypoint_xy[1]) + ","

                k = k + 1

                temp_1 = record[k]
                if k == num_of_records - 1:
                    break
                temp_2 = record[k + 1]

            waypoint_xy = transformer.transform(temp_1['waypoint_latitude'], temp_1['waypoint_longitude'])

            postgres_sql_text = postgres_sql_text + \
                                str(waypoint_xy[0]) + " " + str(waypoint_xy[1]) + ","
            postgres_sql_text = postgres_sql_text + \
                                str(waypoint_xy[0]) + " " + str(waypoint_xy[1]) + ")'), " + \
                                str(UTM_zone) + "), 4326)); "

            cursor_postgres.execute(postgres_sql_text)

            conn_postgres.commit()

            k = k + 1

            if k > num_of_records - 2:
                break

            else:
                temp_1 = record[k]
                temp_2 = record[k + 1]

            if k < num_of_records:

                postgres_sql_text = "INSERT INTO \"" + table_name + "\" " + \
                                    "(\"area_code\"," + \
                                    "\"airport_identifier\"," + \
                                    "\"procedure_identifier\"," + \
                                    "\"transition_identifier\"," + \
                                    "\"geom\")"

                area_code = str(temp_1['area_code'])
                airport_identifier = str(temp_1['airport_identifier'])
                procedure_identifier = str(temp_1['procedure_identifier'])
                transition_identifier = str(temp_1['transition_identifier'])

                UTM_zone = convert_wgs_to_utm(temp_1['waypoint_longitude'], temp_1['waypoint_latitude'])

                transformer = Transformer.from_crs("epsg:4326", "epsg:" + str(UTM_zone))

                waypoint_xy = transformer.transform(temp_1['waypoint_latitude'], temp_1['waypoint_longitude'])

                postgres_sql_text = postgres_sql_text + " VALUES('" + area_code + "','" \
                                    + airport_identifier + "','" \
                                    + procedure_identifier + "','" \
                                    + transition_identifier + "'," \
                                    + "ST_Transform(ST_SetSRID(ST_GeomFromEWKT('CIRCULARSTRING(" \
                                    + str(waypoint_xy[0]) + " " + str(waypoint_xy[1]) + ","

            else:
                break
