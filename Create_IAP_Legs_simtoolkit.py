def create_iap_legs(db_name,schema_name):

    import psycopg2.extras

    #from autoprocess_simtoolkit import db_name, path_db, schema_name

    conn_postgres = psycopg2.connect(
        user='postgres', password='password',
        host='127.0.0.1', port='5432',
        database=db_name,
        options="-c search_path=dbo," + schema_name
    )
    with conn_postgres:
        cursor_postgres = conn_postgres.cursor(cursor_factory=psycopg2.extras.DictCursor)

        table_name = 'iap_legs_without_af_or_rf'

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

        # postgres_sql_text = " SELECT * FROM tbl_iaps " + \
        #                     " where airport_identifier like 'VT%'  " + \
        #                     " and not(waypoint_identifier is null)" + \
        #                     " order by airport_identifier, procedure_identifier, " \
        #                     " route_type, transition_identifier, seqno"

        postgres_sql_text = "SELECT * from tbl_iaps " \
                            "WHERE airport_identifier like '%'" \
                            "and not(waypoint_identifier is null)" \
                            "and NOT(concat(airport_identifier,procedure_identifier,transition_identifier) in " \
                            "(SELECT distinct concat(airport_identifier,procedure_identifier,transition_identifier) from " \
                            "tbl_iaps " \
                            "WHERE path_termination = 'RF')) " \
                            "and NOT(concat(airport_identifier,procedure_identifier,transition_identifier) in " \
                            "(SELECT distinct concat(airport_identifier,procedure_identifier,transition_identifier) from " \
                            "tbl_iaps " \
                            "WHERE path_termination = 'AF') " \
                            "and not(waypoint_identifier is null))" \
                            " order by airport_identifier, procedure_identifier, " \
                            " route_type, transition_identifier, seqno"

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
        route_type = str(temp_1['route_type'])
        transition_identifier = str(temp_1['transition_identifier'])
        seqno = str(temp_1['seqno'])
        waypoint_icao_code = str(temp_1['waypoint_icao_code'])
        waypoint_identifier = str(temp_1['waypoint_identifier'])
        waypoint_latitude = str(temp_1['waypoint_latitude'])
        waypoint_longitude = str(temp_1['waypoint_longitude'])
        waypoint_description_code = str(temp_1['waypoint_description_code'])
        turn_direction = str(temp_1['turn_direction'])
        rnp = str(temp_1['rnp'])
        path_termination = str(temp_1['path_termination'])
        recommanded_navaid = str(temp_1['recommanded_navaid'])
        recommanded_navaid_latitude = str(temp_1['recommanded_navaid_latitude'])
        recommanded_navaid_longitude = str(temp_1['recommanded_navaid_longitude'])
        arc_radius = str(temp_1['arc_radius'])
        theta = str(temp_1['theta'])
        rho = str(temp_1['rho'])
        magnetic_course = str(temp_1['magnetic_course'])
        route_distance_holding_distance_time = str(temp_1['route_distance_holding_distance_time'])
        distance_time = str(temp_1['distance_time'])
        altitude_description = str(temp_1['altitude_description'])
        altitude1 = str(temp_1['altitude1'])
        altitude2 = str(temp_1['altitude2'])
        transition_altitude = str(temp_1['transition_altitude'])
        speed_limit_description = str(temp_1['speed_limit_description'])
        speed_limit = str(temp_1['speed_limit'])
        vertical_angle = str(temp_1['vertical_angle'])
        center_waypoint = str(temp_1['center_waypoint'])
        center_waypoint_latitude = str(temp_1['center_waypoint_latitude'])
        center_waypoint_longitude = str(temp_1['center_waypoint_longitude'])

        postgres_sql_text = "INSERT INTO \"" + table_name + "\" " + \
                            "(\"area_code\"," + \
                            "\"airport_identifier\"," + \
                            "\"procedure_identifier\"," + \
                            "\"transition_identifier\"," + \
                            "\"geom\")"

        postgres_sql_text = postgres_sql_text + " VALUES('" + area_code + "','" \
                            + airport_identifier + "','" \
                            + procedure_identifier + "','" \
                            + transition_identifier + "'," \
                            + "ST_LineFromText('LINESTRING("

        while k < num_of_records - 1:
            while (temp_1['procedure_identifier'] == temp_2['procedure_identifier']) and \
                    (temp_1['transition_identifier'] == temp_2['transition_identifier']) and \
                    not (temp_2['path_termination'] == 'VM') and \
                    (temp_1['path_termination'] == 'TF' or
                     temp_1['path_termination'] == 'DF' or
                     temp_1['path_termination'] == 'CF' or
                     temp_1['path_termination'] == 'FD' or
                     temp_1['path_termination'] == 'FC' or
                     temp_1['path_termination'] == 'FA' or
                     temp_1['path_termination'] == 'IF'):
                postgres_sql_text = postgres_sql_text + \
                                    waypoint_longitude + " " + waypoint_latitude + ","
                k = k + 1
                #print(k)
                temp_1 = record[k]
                if k == num_of_records - 1:
                    break
                temp_2 = record[k + 1]

                waypoint_latitude = str(float(temp_1['waypoint_latitude']))
                waypoint_longitude = str(float(temp_1['waypoint_longitude']))

            postgres_sql_text = postgres_sql_text + \
                                waypoint_longitude + " " + waypoint_latitude + ","

            postgres_sql_text = postgres_sql_text + \
                                waypoint_longitude + " " + waypoint_latitude + ")'" + \
                                ",4326))"

            cursor_postgres.execute(postgres_sql_text)
            #print(postgres_sql_text)

            conn_postgres.commit()
            #print("IAP Legs: " + str("{:.3f}".format((k / num_of_records) * 100, 2)) + "% Completed")

            k = k + 1

            if k > num_of_records - 2:
                break

            else:
                temp_1 = record[k]
                temp_2 = record[k + 1]

            # -----

            latitude_1 = str(float(temp_1['waypoint_latitude']))

            latitude_2 = str(float(temp_2['waypoint_latitude']))

            longitude_1 = str(float(temp_1['waypoint_longitude']))

            longitude_2 = str(float(temp_2['waypoint_longitude']))

            # -----

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
                seqno = str(temp_1['seqno'])
                waypoint_icao_code = str(temp_1['waypoint_icao_code'])
                waypoint_identifier = str(temp_1['waypoint_identifier'])
                waypoint_latitude = str(temp_1['waypoint_latitude'])
                waypoint_longitude = str(temp_1['waypoint_longitude'])
                waypoint_description_code = str(temp_1['waypoint_description_code'])
                turn_direction = str(temp_1['turn_direction'])
                rnp = str(temp_1['rnp'])
                path_termination = str(temp_1['path_termination'])
                recommanded_navaid = str(temp_1['recommanded_navaid'])
                recommanded_navaid_latitude = str(temp_1['recommanded_navaid_latitude'])
                recommanded_navaid_longitude = str(temp_1['recommanded_navaid_longitude'])
                arc_radius = str(temp_1['arc_radius'])
                theta = str(temp_1['theta'])
                rho = str(temp_1['rho'])
                magnetic_course = str(temp_1['magnetic_course'])
                route_distance_holding_distance_time = str(temp_1['route_distance_holding_distance_time'])
                distance_time = str(temp_1['distance_time'])
                altitude_description = str(temp_1['altitude_description'])
                altitude1 = str(temp_1['altitude1'])
                altitude2 = str(temp_1['altitude2'])
                transition_altitude = str(temp_1['transition_altitude'])
                speed_limit_description = str(temp_1['speed_limit_description'])
                speed_limit = str(temp_1['speed_limit'])
                vertical_angle = str(temp_1['vertical_angle'])
                center_waypoint = str(temp_1['center_waypoint'])
                center_waypoint_latitude = str(temp_1['center_waypoint_latitude'])
                center_waypoint_longitude = str(temp_1['center_waypoint_longitude'])

                postgres_sql_text = postgres_sql_text + " VALUES('" + area_code + "','" \
                                    + airport_identifier + "','" \
                                    + procedure_identifier + "','" \
                                    + transition_identifier + "'," \
                                    + "ST_LineFromText('LINESTRING("

            else:
                break
