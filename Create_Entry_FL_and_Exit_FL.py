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
conn_postgres_target = psycopg2.connect(user="de_old_data",
                                        password="de_old_data",
                                        host="172.16.129.241",
                                        port="5432",
                                        database="aerothai_dwh",
                                        options="-c search_path=dbo,public")

# conn_postgres_target = psycopg2.connect(user = "postgres",
#                                   password = "password",
#                                   host = "127.0.0.1",
#                                   port = "5432",
#                                   database = "temp",
#                                   options="-c search_path=dbo,public")

filter = "NOT (latitude is NULL) \n" + \
         "AND NOT(flight_id is NULL) \n" + \
         "AND NOT(geo_alt < 1) \n" \
         "AND ground_speed < 700 \n" \
         "AND ground_speed > 50 \n"

# date_list = pd.date_range(start='2023-07-01', end='2023-07-06')

today = dt.datetime.now()
date_list = [dt.datetime.strptime(f"{today.year}-{today.month}-{today.day}", '%Y-%m-%d') + dt.timedelta(days=-3)]

with conn_postgres_target:
    cursor_postgres_target = conn_postgres_target.cursor()

    for date in date_list:
        year = f"{date.year}"
        month = f"{date.month:02d}"
        day = f"{date.day:02d}"

        t = time.time()
        yyyymmdd = f"{year}{month}{day:}"
        yyyymm = f"{year}{month}"

        print(f"working on creating track_cat62_{yyyymmdd}")

        start_day = dt.datetime.strptime(f"{year}-{month}-{day}", '%Y-%m-%d')
        next_day = start_day + dt.timedelta(days=1)
        previous_day = start_day + dt.timedelta(days=-1)

        year_next = f"{next_day.year}"
        month_next = f"{next_day.month:02d}"
        day_next = f"{next_day.day:02d}"

        yyyymm_next = str(next_day.year).zfill(2) + str(next_day.month).zfill(2)
        yyyymm_previous = str(previous_day.year).zfill(2) + str(previous_day.month).zfill(2)

        yyyymmdd_next = str(next_day.year).zfill(2) + str(next_day.month).zfill(2) + str(next_day.day).zfill(2)
        yyyymmdd_previous = str(previous_day.year).zfill(2) + str(previous_day.month).zfill(2) + str(
            previous_day.day).zfill(2)

        # Create an sql query that creates a new table for radar tracks in the target PostgreSQL database
        postgres_sql_text = f"DROP TABLE IF EXISTS entry_fl_and_exit_fl_{yyyymmdd}_temp; \n" + \
                            f"CREATE TABLE entry_fl_and_exit_fl_{yyyymmdd}_temp " + \
                            "(flight_key character varying, " \
                            "entry_fl double precision, " \
                            "exit_fl double precision)" + \
                            "WITH (OIDS=FALSE);"

        # print(postgres_sql_text)
        cursor_postgres_target.execute(postgres_sql_text)
        conn_postgres_target.commit()

        # Create a connection to the schema in the remote PostgreSQL database
        # where the source data tables are located.
        conn_postgres_source = psycopg2.connect(user="de_old_data",
                                                password="de_old_data",
                                                host="172.16.129.241",
                                                port="5432",
                                                database="aerothai_dwh",
                                                options="-c search_path=dbo,sur_air")

        with conn_postgres_source:

            cursor_postgres_source = conn_postgres_source.cursor(cursor_factory=psycopg2.extras.DictCursor)

            # Create an SQL query that selects surveillance targets from the source PostgreSQL database
            postgres_sql_text = f"SELECT flight_key " \
                                f"FROM flight_data.flight_{yyyymm} " \
                                f"WHERE flight_key LIKE '%{year}-{month}-{day}%' " \
                                f"AND (mapped -> 'TopSky-ATC-MK-CAT062')::integer = 1;"

            #print(postgres_sql_text)

            cursor_postgres_source.execute(postgres_sql_text)
            record = cursor_postgres_source.fetchall()

            num_of_records = len(record)
            k = 0
            temp_1 = record[k]
            flight_key = none_to_null(str(temp_1['flight_key']))
            print(flight_key)


            postgres_sql_text = f"INSERT INTO entry_fl_and_exit_fl_{yyyymmdd}_temp " \
                                f"(\"flight_key\"," + \
                                "\"entry_fl\"," \
                                "\"exit_fl\")"

            postgres_sql_text += " VALUES(" + flight_key + "," \
                                 + entry_fl + "," \
                                 + exit_fl + ");"

            print(postgres_sql_text)
            cursor_postgres_target.execute(postgres_sql_text)
            conn_postgres_target.commit()


        postgres_sql_text = f"DROP TABLE IF EXISTS track.track_{yyyymmdd}_temp;\n" \
                            f"ALTER TABLE public.track_{yyyymmdd}_temp SET SCHEMA track;"
        print(postgres_sql_text)
        cursor_postgres_target.execute(postgres_sql_text)
        conn_postgres_target.commit()

        postgres_sql_text = f"DROP TABLE IF EXISTS track.track_cat62_{yyyymmdd}; \n" \
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

        # Add dest_rwy to the tracks =====================================================

        print(f"working adding arrival runway to {yyyymmdd} tracks")

        postgres_sql_text = f"ALTER TABLE track.track_cat62_{yyyymmdd} \n" \
                            f"DROP COLUMN IF EXISTS dest_rwy;\n" \
                            f"ALTER TABLE track.track_cat62_{yyyymmdd} \n" \
                            f"ADD COLUMN dest_rwy character varying(3) DEFAULT '-';\n"
        cursor_postgres_target.execute(postgres_sql_text)
        conn_postgres_target.commit()

        postgres_sql_text = f"UPDATE track.track_cat62_{yyyymmdd} t\n" \
                            f"SET dest_rwy = f.dest_rwy\n" \
                            f"FROM\n" \
                            f"(SELECT flight_key,dest_rwy\n" \
                            f"FROM\n" \
                            f"(SELECT flight_key,dest,dest_rwy,max(count)\n" \
                            f"FROM\n" \
                            f"(SELECT flight_key,dest, right(procedure_identifier,2) as dest_rwy,COUNT(*)\n" \
                            f"FROM\n" \
                            f"	(SELECT t.flight_key,t.position,t.vert,f.dest,b.airport_identifier,b.procedure_identifier\n" \
                            f"	FROM sur_air.cat062_{yyyymmdd} t, \n" \
                            f"	flight_data.flight_{yyyymm} f,\n" \
                            f"	temp.vt_finalpath_buffer b\n" \
                            f"	WHERE  t.flight_id = f.id\n" \
                            f"	AND (f.dest LIKE 'VT%')\n" \
                            f"	AND ST_INTERSECTS(t.position,b.final_buffer)\n" \
                            f"	UNION\n" \
                            f"	SELECT t.flight_key,t.position,t.vert,f.dest,b.airport_identifier,b.procedure_identifier\n" \
                            f"	FROM sur_air.cat062_{yyyymmdd_next} t, \n" \
                            f"	flight_data.flight_{yyyymm} f,\n" \
                            f"	temp.vt_finalpath_buffer b\n" \
                            f"	WHERE  t.flight_id = f.id\n" \
                            f"	AND (f.dest LIKE 'VT%')\n" \
                            f"	AND ST_INTERSECTS(t.position,b.final_buffer)\n" \
                            f"	) a\n" \
                            f"WHERE dest = airport_identifier AND vert = 2\n" \
                            f"AND length(procedure_identifier) = 3\n" \
                            f"GROUP BY flight_key,dest,procedure_identifier\n" \
                            f"UNION\n" \
                            f"SELECT flight_key,dest, right(procedure_identifier,3) as dest_rwy,COUNT(*) \n" \
                            f"FROM \n" \
                            f"	(SELECT t.flight_key,t.position,t.vert,f.dest,b.airport_identifier,b.procedure_identifier\n" \
                            f"	FROM sur_air.cat062_{yyyymmdd} t, \n" \
                            f"	flight_data.flight_{yyyymm} f,\n" \
                            f"	temp.vt_finalpath_buffer b\n" \
                            f"	WHERE  t.flight_id = f.id\n" \
                            f"	AND (f.dest LIKE 'VT%')\n" \
                            f"	AND ST_INTERSECTS(t.position,b.final_buffer)\n" \
                            f"	UNION\n" \
                            f"	SELECT t.flight_key,t.position,t.vert,f.dest,b.airport_identifier,b.procedure_identifier\n" \
                            f"	FROM sur_air.cat062_{yyyymmdd_next} t, \n" \
                            f"	flight_data.flight_{yyyymm} f,\n" \
                            f"	temp.vt_finalpath_buffer b\n" \
                            f"	WHERE  t.flight_id = f.id\n" \
                            f"	AND (f.dest LIKE 'VT%')\n" \
                            f"	AND ST_INTERSECTS(t.position,b.final_buffer)\n" \
                            f"	) a\n" \
                            f"WHERE dest = airport_identifier AND vert = 2\n" \
                            f"AND length(procedure_identifier) = 4\n" \
                            f"GROUP BY flight_key,dest,procedure_identifier) a\n" \
                            f"GROUP BY flight_key,dest,dest_rwy) b\n" \
                            f") f\n" \
                            f"WHERE  t.flight_key = f.flight_key;\n"
        cursor_postgres_target.execute(postgres_sql_text)
        conn_postgres_target.commit()

        # Add dep to the tracks =====================================================

        print(f"working adding departure runway to {yyyymmdd} tracks")

        postgres_sql_text = f"ALTER TABLE track.track_cat62_{yyyymmdd} \n" \
                            f"DROP COLUMN IF EXISTS dep_rwy;\n" \
                            f"ALTER TABLE track.track_cat62_{yyyymmdd} \n" \
                            f"ADD COLUMN dep_rwy character varying(3) DEFAULT '-';\n"
        cursor_postgres_target.execute(postgres_sql_text)
        conn_postgres_target.commit()

        # Create an SQL query that selects surveillance targets from the source PostgreSQL database
        postgres_sql_text = f"UPDATE track.track_cat62_{yyyymmdd} t\n" \
                            f"SET dep_rwy = f.dep_rwy\n" \
                            f"FROM\n" \
                            f"(SELECT flight_key,dep_rwy\n" \
                            f"FROM\n" \
                            f"(SELECT flight_key,dep,dep_rwy,max(count)\n" \
                            f"FROM\n" \
                            f"(SELECT flight_key,dep, right(runway_identifier,2) as dep_rwy,COUNT(*)\n" \
                            f"FROM\n" \
                            f"	(SELECT t.flight_key,t.position,t.vert,f.dep,b.airport_identifier,b.runway_identifier\n" \
                            f"	FROM sur_air.cat062_{yyyymmdd} t, \n" \
                            f"	flight_data.flight_{yyyymm} f,\n" \
                            f"	temp.vt_dep_buffer b\n" \
                            f"	WHERE  t.flight_id = f.id\n" \
                            f"	AND (f.dep LIKE 'VT%')\n" \
                            f"	AND ST_INTERSECTS(t.position,b.buffer)\n" \
                            f"	) a\n" \
                            f"WHERE dep = airport_identifier AND vert = 1\n" \
                            f"AND length(runway_identifier) = 4\n" \
                            f"GROUP BY flight_key,dep,runway_identifier\n" \
                            f"UNION\n" \
                            f"SELECT flight_key,dep, right(runway_identifier,3) as dep_rwy,COUNT(*) \n" \
                            f"FROM \n" \
                            f"	(SELECT t.flight_key,t.position,t.vert,f.dep,b.airport_identifier,b.runway_identifier\n" \
                            f"	FROM sur_air.cat062_{yyyymmdd} t, \n" \
                            f"	flight_data.flight_{yyyymm} f,\n" \
                            f"	temp.vt_dep_buffer b\n" \
                            f"	WHERE  t.flight_id = f.id\n" \
                            f"	AND (f.dep LIKE 'VT%')\n" \
                            f"	AND ST_INTERSECTS(t.position,b.buffer)\n" \
                            f"	) a\n" \
                            f"WHERE dep = airport_identifier AND vert = 1\n" \
                            f"AND length(runway_identifier) = 5\n" \
                            f"GROUP BY flight_key,dep,runway_identifier) a\n" \
                            f"GROUP BY flight_key,dep,dep_rwy) b\n" \
                            f") f\n" \
                            f"WHERE  t.flight_key = f.flight_key;\n"
        # print(postgres_sql_text)
        cursor_postgres_target.execute(postgres_sql_text)
        conn_postgres_target.commit()
