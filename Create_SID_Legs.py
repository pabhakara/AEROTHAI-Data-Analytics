import psycopg2.extras

# Try to connect to the local PostGresSQL database in which we will store our flight trajectories coupled with FPL data.
conn_postgres = psycopg2.connect(user = "postgres",
                                  password = "password",
                                  host = "127.0.0.1",
                                  port = "5432",
                                  database = "airac_2021_08")
with conn_postgres:
    cursor_postgres = conn_postgres.cursor(cursor_factory = psycopg2.extras.DictCursor)

    table_name = 'sid_legs'

    postgres_sql_text = "\n \n DROP TABLE IF EXISTS " + table_name + "; \n" + \
                        "CREATE TABLE " + table_name + " " + \
                        "(procedure_ident character varying, " + \
                        "airport_ident character varying, " + \
                        "proc_type character varying, " + \
                        "rwy_designator character varying, " + \
                        "transition character varying," \
                        "geom geometry)" + \
                        "WITH (OIDS=FALSE); \n" + \
                        "ALTER TABLE " + table_name + " " \
                        "OWNER TO postgres;"

    print(postgres_sql_text)

    cursor_postgres.execute(postgres_sql_text)

    conn_postgres.commit()

    postgres_sql_text = "select ST_Y(wp_geom) as \"Latitude\", ST_X(wp_geom) as \"Longitude\", * " + \
                        "from sid_wp order by airport_ident,procedure_ident,\"Transition\",\"ID\""

    print(postgres_sql_text)

    cursor_postgres.execute(postgres_sql_text)

    record = cursor_postgres.fetchall()

    num_of_records = len(record)
    print("num_of_record: ",num_of_records)

    cursor_postgres = conn_postgres.cursor()

    k = 0

    temp_1 = record[k]
    temp_2 = record[k + 1]

    airport_ident = str(temp_1['airport_ident'])
    proc_type = str(temp_1['procedure_type'])
    procedure_ident = str(temp_1['procedure_ident'])
    rwy_designator = str(temp_1['rwy_designator'])
    transition = str(temp_1['Transition'])
    latitude_1 = str(float(temp_1['Latitude']))
    longitude_1 = str(float(temp_1['Longitude']))

    postgres_sql_text = "INSERT INTO \"" + table_name + "\" (\"airport_ident\"," + \
                        "\"proc_type\",\"procedure_ident\",\"rwy_designator\",\"transition\",\"geom\")"

    postgres_sql_text = postgres_sql_text + " VALUES('" + airport_ident + "','" \
                        + proc_type + "','" \
                        + procedure_ident + "','" \
                        + rwy_designator + "','" \
                        + transition + "'," \
                        + "ST_LineFromText('LINESTRING("

    while k < num_of_records - 1:
        while (temp_1['procedure_ident'] == temp_2['procedure_ident']):
            postgres_sql_text = postgres_sql_text + \
                                longitude_1 + " " + latitude_1 +  ","
            k = k + 1
            print(k)

            if k > num_of_records - 2:
                break
            else:
                temp_1 = record[k]
                temp_2 = record[k+1]
                latitude_1 = str(float(temp_1['Latitude']))
                longitude_1 = str(float(temp_1['Longitude']))

        postgres_sql_text = postgres_sql_text + \
                            longitude_1 + " " + latitude_1 + ","

        postgres_sql_text = postgres_sql_text + \
                            longitude_1 + " " + latitude_1 + ")'" + \
                            ",4326))"

        print(postgres_sql_text)
        cursor_postgres.execute(postgres_sql_text)
        conn_postgres.commit()
        print(str("{:.3f}".format((k / num_of_records) * 100, 2)) + "% Completed")

        k = k + 1

        if k > num_of_records - 2:
            break
        else:
            temp_1 = record[k]
            temp_2 = record[k + 1]

        # -----

        latitude_1 = str(float(temp_1['Latitude']))

        latitude_2 = str(float(temp_2['Latitude']))

        longitude_1 = str(float(temp_1['Longitude']))

        longitude_2 = str(float(temp_2['Longitude']))

        # -----

        if k < num_of_records:

            postgres_sql_text = "INSERT INTO \"" + table_name + "\" (\"airport_ident\"," + \
                                "\"proc_type\",\"procedure_ident\",\"rwy_designator\",\"transition\",\"geom\")"

            airport_ident = str(temp_1['airport_ident'])
            proc_type = str(temp_1['procedure_type'])
            procedure_ident = str(temp_1['procedure_ident'])
            rwy_designator = str(temp_1['rwy_designator'])
            transition = str(temp_1['Transition'])
            latitude_1 = str(float(temp_1['Latitude']))
            longitude_1 = str(float(temp_1['Longitude']))

            postgres_sql_text = postgres_sql_text + " VALUES('" + airport_ident + "','" \
                                + proc_type + "','" \
                                + procedure_ident + "','" \
                                + rwy_designator + "','" \
                                + transition + "'," \
                                + "ST_LineFromText('LINESTRING("
        else:
            break