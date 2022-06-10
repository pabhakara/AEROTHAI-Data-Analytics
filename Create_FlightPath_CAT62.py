import psycopg2
import psycopg2.extras
import time

def none_to_null(etd):
    if etd == 'None':
        x = 'null'
    else:
        x = "'" + etd + "'"
    return x

# Try to connect to the remote PostGresSQL database in which we will store our flight trajectories coupled with FPL data.
conn_postgres_target = psycopg2.connect(user = "de_old_data",
                                  password = "de_old_data",
                                  host = "172.16.129.241",
                                  port = "5432",
                                  database = "aerothai_dwh",
                                  options="-c search_path=dbo,public")

with conn_postgres_target:

    cursor_postgres_target = conn_postgres_target.cursor()

    year_list = ['2022']
    month_list = ['05']
    day_list = ['01','02','03','04','05','06']

    for year in year_list:
        for month in month_list:
            for day in day_list:
                t = time.time()

                yyyymmdd = year + month + day



                table_name = "track_cat062_" + yyyymmdd + ""

                print(table_name)

                # Create an sql query that creates a new table for radar tracks in Postgres SQL database
                postgres_sql_text = "DROP TABLE IF EXISTS " + table_name + "; \n" + \
                                    "CREATE TABLE " + table_name + " " + \
                                    "(acid character varying, " \
                                    "track_no integer, " \
                                    "geom geometry, " + \
                                    "start_time timestamp without time zone, " + \
                                    "end_time timestamp without time zone, " + \
                                    "icao_24bit_dap character varying," + \
                                    "mode_a_code character varying," + \
                                    "dep character varying, " \
                                    "dest character varying)" + \
                                    "WITH (OIDS=FALSE);"

                #print(postgres_sql_text)
                cursor_postgres_target.execute(postgres_sql_text)
                conn_postgres_target.commit()

                conn_postgres_source = psycopg2.connect(user="de_old_data",
                                             password="de_old_data",
                                             host="172.16.129.241",
                                             port="5432",
                                             database="aerothai_dwh",
                                             options="-c search_path=dbo,sur_air")

                with conn_postgres_source:

                    cursor_postgres_source = conn_postgres_source.cursor(cursor_factory=psycopg2.extras.DictCursor)

                    postgres_sql_text = "SELECT track_no, " + \
                                        "time_of_track," + \
                                        "icao_24bit_dap," + \
                                        "mode_a_code," + \
                                        "acid," + \
                                        "acid_dap," + \
                                        "dep," + \
                                        "dest," + \
                                        "latitude," + \
                                        "longitude," + \
                                        "measured_fl " + \
                                        "FROM sur_air.cat062_" + yyyymmdd + " " + \
                                        "WHERE not(latitude is NULL) " + \
                                        "AND NOT(dep is NULL) " + \
                                        "ORDER BY track_no, time_of_track ASC"

                    #print(postgres_sql_text)

                    cursor_postgres_source.execute(postgres_sql_text)
                    record = cursor_postgres_source.fetchall()
                    #print(record)

                    num_of_records = len(record)
                    #print("num_of_record: ",num_of_records)

                    k = 0

                    temp_1 = record[k]
                    #print(temp_1)
                    temp_2 = record[k+1]

                    acid = none_to_null(str(temp_1['acid']))

                    app_time = str(temp_1['time_of_track'])

                    dep = none_to_null(str(temp_1['dep']))
                    dest = none_to_null(str(temp_1['dest']))

                    track_no = none_to_null(str(temp_1['track_no']))

                    icao_24bit_dap = none_to_null(str(temp_1['icao_24bit_dap']))
                    mode_a_code = none_to_null(str(temp_1['mode_a_code']))

                    measured_fl = none_to_null(str(temp_1['measured_fl']))

                    track_no_1 = str(temp_1['track_no'])
                    track_no_2 = str(temp_2['track_no'])

                    latitude_1 = str(float(temp_1['latitude']))
                    latitude_2 = str(float(temp_2['latitude']))

                    longitude_1 = str(float(temp_1['longitude']))
                    longitude_2 = str(float(temp_1['longitude']))


                    postgres_sql_text = "INSERT INTO \"" + table_name + "\" (\"acid\"," + \
                                        "\"track_no\"," \
                                        "\"icao_24bit_dap\"," \
                                        "\"mode_a_code\", \"start_time\"," + \
                                        "\"dep\",\"dest\"," + \
                                        "\"geom\",\"end_time\")"

                    postgres_sql_text += " VALUES(" + acid + "," \
                                         + track_no + "," \
                                         + icao_24bit_dap + "," \
                                         + mode_a_code + ",'" \
                                         + app_time + "'," \
                                         + dep + "," \
                                         + dest + "," \
                                        + "ST_LineFromText('LINESTRING("

                    app_time_1 = str(temp_1['time_of_track'])
                    app_time_2 = str(temp_2['time_of_track'])

                    measured_fl_1 = str(temp_1['measured_fl'])
                    if measured_fl_1 == 'None':
                        measured_fl_1 = "-1"
                    measured_fl_2 = str(temp_2['measured_fl'])
                    if measured_fl_2 == 'None':
                        measured_fl_2 = "-1"

                    while k < num_of_records - 1:
                        while (temp_1['track_no'] == temp_2['track_no']) and \
                            abs(temp_2['longitude'] - temp_1['longitude']) < 1 and \
                            abs(temp_2['latitude'] - temp_1['latitude']) < 1:
                            #(temp_2['app_time'] - temp_1['app_time']) <= datetime.timedelta(minutes=1) and \
                            #abs(temp_2['longitude'] - temp_1['longitude']) < 1 and \
                            #abs(temp_2['latitude'] - temp_1['latitude']) < 1:

                            postgres_sql_text = postgres_sql_text + \
                                                longitude_1 + " " + latitude_1 + " " + \
                                                measured_fl_1 + ","
                            k = k + 1
                            if k == num_of_records-1:
                                break
                            temp_1 = record[k]

                            latitude_1 = str(float(temp_1['latitude']))
                            longitude_1 = str(float(temp_1['longitude']))
                            app_time_1 = str(temp_1['time_of_track'])

                            temp_2 = record[k + 1]

                        postgres_sql_text += longitude_1 + " " + latitude_1 + " " + \
                                                measured_fl_1 + ","

                        postgres_sql_text += longitude_1 + " " + latitude_1 + " " + \
                                            measured_fl_1 + ")',4326),'"

                        postgres_sql_text += app_time_1 +"')"

                        #print(str(temp_1['dep']))
                        #print(str(temp_1['dest']))

                        #print(postgres_sql_text)
                        cursor_postgres_target.execute(postgres_sql_text)
                        conn_postgres_target.commit()
                        print(str("{:.3f}".format((k / num_of_records) * 100,2)) + "% Completed")

                        if num_of_records - k <= 2:
                            # Calculate track duration and track distance at the end
                            postgres_sql_text = "DROP TABLE IF EXISTS " + table_name + "_length; \n" + \
                                                "SELECT *, end_time-start_time as track_duration, " \
                                                "ST_LengthSpheroid(geom, 'SPHEROID[\"WGS 84\",6378137,298.257223563]') / 1852 as track_length " + \
                                                "INTO " + table_name + "_length from " + table_name + ";" + \
                                                "DROP TABLE IF EXISTS " + table_name + ";" + \
                                                "ALTER TABLE " + table_name + "_length RENAME TO " + table_name + ";"

                            #print(postgres_sql_text)
                            cursor_postgres_target.execute(postgres_sql_text)
                            conn_postgres_target.commit()
                            break

                        k = k + 1
                        temp_1 = record[k]
                        temp_2 = record[k + 1]

                        #-----

                        latitude_1 = str(float(temp_1['latitude']))
                        latitude_2 = str(float(temp_2['latitude']))

                        longitude_1 = str(float(temp_1['longitude']))
                        longitude_2 = str(float(temp_1['longitude']))

                        app_time_1 = str(temp_1['time_of_track'])
                        app_time_2 = str(temp_2['time_of_track'])

                        measured_fl_1 = str(temp_1['measured_fl'])
                        if measured_fl_1 == 'None':
                            measured_fl_1 = "-1"
                        measured_fl_2 = str(temp_2['measured_fl'])
                        if measured_fl_2 == 'None':
                            measured_fl_2 = "-1"

                        #-----

                        if k < num_of_records:

                            postgres_sql_text = "INSERT INTO \"" + table_name + "\" (\"acid\"," + \
                                                "\"track_no\"," \
                                                "\"icao_24bit_dap\"," \
                                                "\"mode_a_code\", \"start_time\"," + \
                                                "\"dep\",\"dest\"," + \
                                                "\"geom\",\"end_time\")"

                            acid = none_to_null(str(temp_1['acid']))

                            app_time = str(temp_1['time_of_track'])

                            dep = none_to_null(str(temp_1['dep']))
                            dest = none_to_null(str(temp_1['dest']))
                            icao_24bit_dap = none_to_null(str(temp_1['icao_24bit_dap']))
                            mode_a_code = none_to_null(str(temp_1['mode_a_code']))

                            track_no = str(temp_1['track_no'])

                            postgres_sql_text += " VALUES(" + acid + "," \
                                                 + track_no + "," \
                                                 + icao_24bit_dap + "," \
                                                 + mode_a_code + ",'" \
                                                 + app_time + "'," \
                                                 + dep + "," \
                                                 + dest + "," \
                                                 + "ST_LineFromText('LINESTRING("
                        else:
                            break

    postgres_sql_text = "DO \n" \
                        "$$ \n" \
                        "DECLARE \n" \
                        " row record; \n" \
                        "BEGIN \n" \
                        "FOR row IN SELECT tablename FROM pg_tables WHERE schemaname = 'public' and tablename like 'track%' \n" \
                        "LOOP \n" \
                        "EXECUTE 'ALTER TABLE public.' || quote_ident(row.tablename) || ' SET SCHEMA track;'; \n" \
                        "END LOOP; \n" \
                        "END; \n" \
                        "$$;"
    cursor_postgres_target.execute(postgres_sql_text)
    conn_postgres_target.commit()