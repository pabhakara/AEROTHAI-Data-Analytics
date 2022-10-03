import psycopg2
import psycopg2.extras
import time
import datetime as dt
import pandas as pd

def none_to_null(etd):
    if etd == 'None':
        x = 'null'
    else:
        x = "'" + etd + "'"
    return x

# Create a connection to the remote PostGresSQL database in which we will store our trajectories
# created from ASTERIX Cat062 targets.
conn_postgres_target = psycopg2.connect(user = "de_old_data",
                                  password = "de_old_data",
                                  host = "172.16.129.241",
                                  port = "5432",
                                  database = "aerothai_dwh",
                                  options="-c search_path=dbo,public")

# conn_postgres_target = psycopg2.connect(user = "postgres",
#                                   password = "password",
#                                   host = "127.0.0.1",
#                                   port = "5432",
#                                   database = "temp",
#                                   options="-c search_path=dbo,public")

filter =  "NOT (latitude is NULL) \n" + \
          "AND NOT(flight_id is NULL) \n" + \
          "AND NOT(geo_alt < 1) \n" \
          "AND ground_speed < 700 \n" \
          "AND ground_speed > 50 \n"


date_list = pd.date_range(start='2022-09-30', end='2022-09-30')

with conn_postgres_target:

    cursor_postgres_target = conn_postgres_target.cursor()

    for date in date_list:
        year = f"{date.year}"
        month = f"{date.month:02d}"
        day = f"{date.day:02d}"

    # year_list = ['2022']
    # month_list = ['09']
    # # day_list = [ '01','02', '03', '04', '05','06','07','08','09','10',
    # #             '11', '12', '13', '14', '15', '16', '17', '18', '19', '20',
    # #             '21', '22', '23', '24', '25','26','27','28','29','30','31']
    # day_list = ['25']
    # for year in year_list:
    #     for month in month_list:
    #         yyyymm = year + month
    #         for day in day_list:
        t = time.time()
        yyyymmdd = f"{year}{month}{day:}"
        yyyymm = f"{year}{month}"

        print(f"working on {yyyymmdd}")

        start_day = dt.datetime.strptime(f"{year}-{month}-{day}", '%Y-%m-%d')
        next_day = start_day + dt.timedelta(days=1)
        previous_day = start_day + dt.timedelta(days=-1)

        yyyymmdd_next = str(next_day.year).zfill(2) + str(next_day.month).zfill(2) + str(next_day.day).zfill(2)
        yyyymmdd_previous = str(previous_day.year).zfill(2) + str(previous_day.month).zfill(2) + str(previous_day.day).zfill(2)

        # Create an sql query that creates a new table for radar tracks in the target PostgreSQL database
        postgres_sql_text = f"DROP TABLE IF EXISTS track_{yyyymmdd}_temp; \n" + \
                            f"CREATE TABLE track_{yyyymmdd}_temp " + \
                            "(acid character varying, " \
                            "track_no integer, " \
                            "geom geometry, " + \
                            "start_time timestamp without time zone, " + \
                            "end_time timestamp without time zone, " + \
                            "icao_24bit_dap character varying," + \
                            "mode_a_code character varying," + \
                            "dep character varying, " \
                            "dest character varying," \
                            "flight_id integer," \
                            "flight_key character varying)" + \
                            "WITH (OIDS=FALSE);"

        #print(postgres_sql_text)
        cursor_postgres_target.execute(postgres_sql_text)
        conn_postgres_target.commit()

        # Create a connection to the schema in the remote PostgreSQL database
        # where the source data tables are located.
        conn_postgres_source = psycopg2.connect(user="pongabhaab",
                                     password="pongabhaab",
                                     host="172.16.129.241",
                                     port="5432",
                                     database="aerothai_dwh",
                                     options="-c search_path=dbo,sur_air")

        with conn_postgres_source:

            cursor_postgres_source = conn_postgres_source.cursor(cursor_factory=psycopg2.extras.DictCursor)

            # Create an SQL query that selects surveillance targets from the source PostgreSQL database
            postgres_sql_text = "SELECT track_no, " + \
                                "time_of_track," + \
                                "icao_24bit_dap," + \
                                "mode_a_code," + \
                                "acid," + \
                                "acid_dap," + \
                                "dep," + \
                                "dest," + \
                                "flight_key," + \
                                "flight_id," + \
                                "latitude," + \
                                "longitude," + \
                                "geo_alt \n" + \
                                "FROM sur_air.cat062_" + yyyymmdd_next + "\n " + \
                                f"WHERE \n {filter}" + \
                                "AND concat(track_no, acid, mode_a_code) \n" + \
                                "IN \n" \
                                "(SELECT distinct concat(track_no, acid, mode_a_code) \n" \
                                "from \n" \
                                "(SELECT track_no, acid, mode_a_code, count(*) \n" \
                                "from \n" \
                                "(SELECT track_no, acid, mode_a_code, temp, count(*) \n" \
                                "from \n" \
                                "(SELECT 'a' as temp, * FROM sur_air.cat062_" + yyyymmdd_next + \
                                "\n WHERE app_time < \'" + \
                                str(next_day.year).zfill(2) + "-" + \
                                str(next_day.month).zfill(2) + "-" + \
                                str(next_day.day).zfill(2) + \
                                " 00:00:30\' \n " \
                                f"AND {filter} \n" \
                                "UNION \n" \
                                "SELECT 'b' as temp, * \n FROM " \
                                "sur_air.cat062_" + yyyymmdd + "\n" \
                                f"WHERE \n {filter}" \
                                "AND app_time > \'" + year + "-" + month + "-" + day + " 23:59:30\' \n" \
                                "ORDER BY track_no, app_time ) a \n" \
                                "GROUP BY track_no, acid, mode_a_code, temp) b \n" \
                                "GROUP BY track_no, acid, mode_a_code) c \n" \
                                "WHERE count = 2) \n" \
                                "UNION \n" \
                                "SELECT " \
                                "track_no, time_of_track, icao_24bit_dap, mode_a_code, acid, " \
                                "acid_dap, dep, dest, flight_key, flight_id, latitude, longitude, geo_alt \n" \
                                "FROM sur_air.cat062_" + yyyymmdd + "\n" \
                                f"WHERE \n {filter}" \
                                "AND NOT concat(track_no, acid, mode_a_code) IN \n" \
                                "(SELECT distinct concat(track_no, acid, mode_a_code) \n" \
                                "from \n" \
                                "(SELECT track_no, acid, mode_a_code, count(*) \n" \
                                "from \n" \
                                "(SELECT track_no, acid, mode_a_code, temp, count(*) \n" \
                                "from \n" \
                                "(SELECT 'a' as temp, * FROM sur_air.cat062_" + yyyymmdd_previous + "\n"\
                                f"WHERE \n {filter}" \
                                "AND app_time > \'" + \
                                str(previous_day.year).zfill(2) + "-" + \
                                str(previous_day.month).zfill(2) + "-" + \
                                str(previous_day.day).zfill(2) + \
                                " 23:59:30\' \n " \
                                "UNION \n" \
                                "SELECT 'b' as temp, * \n FROM " \
                                "sur_air.cat062_" + yyyymmdd + "\n" \
                                f"WHERE \n {filter}" \
                                "AND app_time < \'" + year + "-" + month + "-" + day + " 00:00:30\' \n" \
                                "ORDER BY track_no, app_time ) a \n" \
                                "GROUP BY track_no, acid, mode_a_code, temp) b \n" \
                                "GROUP BY track_no, acid, mode_a_code) c \n" \
                                "WHERE count = 2) \n" \
                                "ORDER BY track_no, time_of_track ASC;"

            cursor_postgres_source.execute(postgres_sql_text)
            record = cursor_postgres_source.fetchall()

            num_of_records = len(record)

            k = 0

            print(postgres_sql_text)

            temp_1 = record[k]
            temp_2 = record[k+1]

            acid = none_to_null(str(temp_1['acid']))

            app_time = str(temp_1['time_of_track'])

            dep = none_to_null(str(temp_1['dep']))
            dest = none_to_null(str(temp_1['dest']))

            flight_key = none_to_null(str(temp_1['flight_key']))
            flight_id = none_to_null(str(temp_1['flight_id']))

            track_no = none_to_null(str(temp_1['track_no']))

            icao_24bit_dap = none_to_null(str(temp_1['icao_24bit_dap']))
            mode_a_code = none_to_null(str(temp_1['mode_a_code']))

            geo_alt = none_to_null(str(temp_1['geo_alt']))

            track_no_1 = str(temp_1['track_no'])
            track_no_2 = str(temp_2['track_no'])

            latitude_1 = str(float(temp_1['latitude']))
            latitude_2 = str(float(temp_2['latitude']))

            longitude_1 = str(float(temp_1['longitude']))
            longitude_2 = str(float(temp_1['longitude']))


            postgres_sql_text = f"INSERT INTO \"track_{yyyymmdd}_temp\" (\"acid\"," + \
                                "\"track_no\"," \
                                "\"icao_24bit_dap\"," \
                                "\"mode_a_code\", \"start_time\"," + \
                                "\"dep\",\"dest\",\"flight_key\",\"flight_id\"," + \
                                "\"geom\",\"end_time\")"

            postgres_sql_text += " VALUES(" + acid + "," \
                                 + track_no + "," \
                                 + icao_24bit_dap + "," \
                                 + mode_a_code + ",'" \
                                 + app_time + "'," \
                                 + dep + "," \
                                 + dest + "," \
                                 + flight_key + "," \
                                 + flight_id + "," \
                                 + "ST_LineFromText('LINESTRING("

            app_time_1 = str(temp_1['time_of_track'])
            app_time_2 = str(temp_2['time_of_track'])

            geo_alt_1 = str(temp_1['geo_alt'])
            if geo_alt_1 == 'None':
                geo_alt_1 = "-1"
            geo_alt_2 = str(temp_2['geo_alt'])
            if geo_alt_2 == 'None':
                geo_alt_2 = "-1"

            while k < num_of_records - 1:
                while (temp_1['track_no'] == temp_2['track_no']) and \
                    abs(temp_2['longitude'] - temp_1['longitude']) < 1 and \
                    abs(temp_2['latitude'] - temp_1['latitude']) < 1 and \
                    (temp_2['time_of_track'] - temp_1['time_of_track']) <= dt.timedelta(minutes=1):
                    #abs(temp_2['longitude'] - temp_1['longitude']) < 1 and \
                    #abs(temp_2['latitude'] - temp_1['latitude']) < 1:

                    postgres_sql_text = postgres_sql_text + \
                                        longitude_1 + " " + latitude_1 + " " + \
                                        geo_alt_1 + ","
                    k = k + 1
                    if k == num_of_records-1:
                        break
                    temp_1 = record[k]

                    latitude_1 = str(float(temp_1['latitude']))
                    longitude_1 = str(float(temp_1['longitude']))
                    app_time_1 = str(temp_1['time_of_track'])

                    temp_2 = record[k + 1]

                postgres_sql_text += longitude_1 + " " + latitude_1 + " " + \
                                        geo_alt_1 + ","

                postgres_sql_text += longitude_1 + " " + latitude_1 + " " + \
                                    geo_alt_1 + ")',4326),'"

                postgres_sql_text += app_time_1 +"')"

                cursor_postgres_target.execute(postgres_sql_text)
                conn_postgres_target.commit()
                print('track_' + yyyymmdd + str(" {:.3f}".format((k / num_of_records) * 100,2)) + "% Completed")

                if num_of_records - k <= 2:
                    # Calculate track duration and track distance at the end
                    postgres_sql_text = f"DROP TABLE IF EXISTS track_{yyyymmdd}_temp_length; \n" + \
                                        "SELECT *, end_time-start_time as track_duration, " \
                                        "ST_LengthSpheroid(geom, 'SPHEROID[\"WGS 84\",6378137,298.257223563]') / 1852 as track_length " + \
                                        f"INTO track_{yyyymmdd}_temp_length from track_{yyyymmdd}_temp;" + \
                                        f"DROP TABLE IF EXISTS track_{yyyymmdd}_temp;" + \
                                        f"ALTER TABLE track_{yyyymmdd}_temp_length RENAME TO track_{yyyymmdd}_temp;"

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

                geo_alt_1 = str(temp_1['geo_alt'])
                if geo_alt_1 == 'None':
                    geo_alt_1 = "-1"
                geo_alt_2 = str(temp_2['geo_alt'])
                if geo_alt_2 == 'None':
                    geo_alt_2 = "-1"

                if k < num_of_records:

                    postgres_sql_text = f"INSERT INTO \"track_{yyyymmdd}_temp\" (\"acid\"," + \
                                        "\"track_no\"," \
                                        "\"icao_24bit_dap\"," \
                                        "\"mode_a_code\", \"start_time\"," + \
                                        "\"dep\",\"dest\",\"flight_key\",\"flight_id\"," + \
                                        "\"geom\",\"end_time\")"

                    acid = none_to_null(str(temp_1['acid']))

                    app_time = str(temp_1['time_of_track'])

                    dep = none_to_null(str(temp_1['dep']))
                    dest = none_to_null(str(temp_1['dest']))

                    flight_key = none_to_null(str(temp_1['flight_key']))
                    flight_id = none_to_null(str(temp_1['flight_id']))

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
                                         + flight_key + "," \
                                         + flight_id + "," \
                                         + "ST_LineFromText('LINESTRING("
                else:
                    break

        postgres_sql_text = f"DROP TABLE IF EXISTS track.track_{yyyymmdd}_temp;\n"  \
                            f"ALTER TABLE public.track_{yyyymmdd}_temp SET SCHEMA track;"
        print(postgres_sql_text)
        cursor_postgres_target.execute(postgres_sql_text)
        conn_postgres_target.commit()

        postgres_sql_text = f"DROP TABLE IF EXISTS track.track_cat62_{yyyymmdd}; \n"  \
                            f"SELECT track.track_{yyyymmdd}_temp.geom," \
                            f"track.track_{yyyymmdd}_temp.start_time," \
                            f"track.track_{yyyymmdd}_temp.end_time," \
                            f"track.track_{yyyymmdd}_temp.track_duration," \
                            f"track.track_{yyyymmdd}_temp.track_length," \
                            f"flight_data.flight_{yyyymm}.* " \
                            f"INTO track.track_cat62_{yyyymmdd} " \
                            f"FROM track.track_{yyyymmdd}_temp " \
                            f"LEFT JOIN flight_data.flight_{yyyymm} ON " \
                            f"track.track_{yyyymmdd}_temp.flight_key = flight_data.flight_{yyyymm}.flight_key;"

        print(postgres_sql_text)
        cursor_postgres_target.execute(postgres_sql_text)
        conn_postgres_target.commit()

        postgres_sql_text = f"DROP TABLE IF EXISTS track.track_{yyyymmdd}_temp;" \
                            f"GRANT SELECT ON ALL TABLES IN SCHEMA track TO public;"
        print(postgres_sql_text)
        cursor_postgres_target.execute(postgres_sql_text)
        conn_postgres_target.commit()


