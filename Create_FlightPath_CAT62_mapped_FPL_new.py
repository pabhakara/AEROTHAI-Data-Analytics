import datetime as dt
import time
from subprocess import PIPE, Popen

import pandas as pd
import psycopg2
import psycopg2.extras


def none_to_null(etd):
    return 'null' if etd == 'None' else f"'{etd}'"


# Create a connection to the remote PostgreSQL database
conn_postgres_target = psycopg2.connect(
    user="de_old_data",
    password="de_old_data",
    host="172.16.129.241",
    port="5432",
    database="aerothai_dwh",
    options="-c search_path=dbo,public"
)

filter = (
    "NOT (latitude is NULL) \n"
    "AND NOT(flight_id is NULL) \n"
    "AND NOT(geo_alt < 1) \n"
    "AND ground_speed < 700 \n"
    "AND ground_speed > 50 \n"
)

date_list = pd.date_range(start='2024-06-02', end='2024-06-02')

# today = dt.datetime.now()
# date_list = [
#     dt.datetime.strptime(f"{today.year}-{today.month}-{today.day}", '%Y-%m-%d') + dt.timedelta(days=-3)
# ]

track_suffix = ""
sur_air_suffix = ""

with conn_postgres_target:
    cursor_postgres_target = conn_postgres_target.cursor()

    for date in date_list:
        year = f"{date.year}"
        month = f"{date.month:02d}"
        day = f"{date.day:02d}"

        yyyymmdd = f"{year}{month}{day}"
        yyyymm = f"{year}{month}"

        print(f"working on creating track_cat62_{yyyymmdd}{track_suffix}")

        start_day = dt.datetime.strptime(f"{year}-{month}-{day}", '%Y-%m-%d')
        next_day = start_day + dt.timedelta(days=1)
        previous_day = start_day + dt.timedelta(days=-1)

        yyyymm_next = f"{next_day.year:02d}{next_day.month:02d}"
        yyyymm_previous = f"{previous_day.year:02d}{previous_day.month:02d}"

        yyyymmdd_next = f"{next_day.year:02d}{next_day.month:02d}{next_day.day:02d}"
        yyyymmdd_previous = f"{previous_day.year:02d}{previous_day.month:02d}{previous_day.day:02d}"

        # Create an SQL query to create a new table for radar tracks in the target PostgreSQL database
        postgres_sql_text = (
            f"DROP TABLE IF EXISTS track_{yyyymmdd}{track_suffix}_temp; \n"
            f"CREATE TABLE track_{yyyymmdd}{track_suffix}_temp ("
            "acid character varying, "
            "track_no integer, "
            "geom geometry, "
            "start_time timestamp without time zone, "
            "end_time timestamp without time zone, "
            "icao_24bit_dap character varying, "
            "mode_a_code character varying, "
            "dep character varying, "
            "dest character varying, "
            "flight_id integer, "
            "flight_key character varying) "
            "WITH (OIDS=FALSE);"
        )

        cursor_postgres_target.execute(postgres_sql_text)
        conn_postgres_target.commit()

        # Create a connection to the schema in the remote PostgreSQL database
        conn_postgres_source = psycopg2.connect(
            user="de_old_data",
            password="de_old_data",
            host="172.16.129.241",
            port="5432",
            database="aerothai_dwh",
            options="-c search_path=dbo,sur_air"
        )

        with conn_postgres_source:
            cursor_postgres_source = conn_postgres_source.cursor(cursor_factory=psycopg2.extras.DictCursor)

            # Create an SQL query that selects surveillance targets from the source PostgreSQL database
            postgres_sql_text = (
                f"SELECT * FROM ("
                f"SELECT track_no, time_of_track, icao_24bit_dap, mode_a_code, acid, acid_dap, dep, dest, flight_key, "
                f"flight_id, latitude, longitude, geo_alt "
                f"FROM sur_air.cat062_{yyyymmdd}{sur_air_suffix} "
                f"WHERE NOT (latitude is NULL) "
                f"AND NOT(flight_id is NULL) "
                f"AND NOT(geo_alt < 1) "
                f"AND ground_speed < 700 "
                f"AND ground_speed > 50 "
                f"AND flight_key LIKE '%{year}-{month}-{day}%' "
                f"UNION "
                f"SELECT track_no, time_of_track, icao_24bit_dap, mode_a_code, acid, acid_dap, dep, dest, flight_key, "
                f"flight_id, latitude, longitude, geo_alt "
                f"FROM sur_air.cat062_{yyyymmdd_next}{sur_air_suffix} "
                f"WHERE NOT (latitude is NULL) "
                f"AND NOT(flight_id is NULL) "
                f"AND NOT(geo_alt < 1) "
                f"AND ground_speed < 700 "
                f"AND ground_speed > 50 "
                f"AND flight_key LIKE '%{year}-{month}-{day}%') a "
                f"ORDER BY flight_key, time_of_track ASC;"
            )

            cursor_postgres_source.execute(postgres_sql_text)
            record = cursor_postgres_source.fetchall()

            num_of_records = len(record)
            k = 0

            while k < num_of_records - 1:
                temp_1 = record[k]
                temp_2 = record[k + 1]

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

                latitude_1 = str(float(temp_1['latitude']))
                longitude_1 = str(float(temp_1['longitude']))
                geo_alt_1 = str(temp_1['geo_alt'])
                app_time_1 = str(temp_1['time_of_track'])

                latitude_2 = str(float(temp_2['latitude']))
                longitude_2 = str(float(temp_2['longitude']))
                geo_alt_2 = str(temp_2['geo_alt'])
                app_time_2 = str(temp_2['time_of_track'])

                postgres_sql_text = (
                    f"INSERT INTO track_{yyyymmdd}{track_suffix}_temp (acid, track_no, icao_24bit_dap, mode_a_code, "
                    f"start_time, dep, dest, flight_key, flight_id, geom, end_time) "
                    f"VALUES ({acid}, {track_no}, {icao_24bit_dap}, {mode_a_code}, '{app_time}', {dep}, {dest}, "
                    f"{flight_key}, {flight_id}, ST_LineFromText('LINESTRING("
                )

                while (temp_1['track_no'] == temp_2['track_no'] and
                       abs(temp_2['longitude'] - temp_1['longitude']) < 1 and
                       abs(temp_2['latitude'] - temp_1['latitude']) < 1 and
                       (temp_2['time_of_track'] - temp_1['time_of_track']) <= dt.timedelta(minutes=1)):
                    postgres_sql_text += f"{longitude_1} {latitude_1} {geo_alt_1},"
                    k += 1
                    if k == num_of_records - 1:
                        break
                    temp_1 = record[k]
                    latitude_1 = str(float(temp_1['latitude']))
                    longitude_1 = str(float(temp_1['longitude']))
                    geo_alt_1 = str(temp_1['geo_alt'])
                    app_time_1 = str(temp_1['time_of_track'])
                    temp_2 = record[k + 1]

                postgres_sql_text += f"{longitude_1} {latitude_1} {geo_alt_1},"
                postgres_sql_text += (
                    f"{longitude_1} {latitude_1} {geo_alt_1})',4326),'"
                    f"{app_time_1}')"
                )

                cursor_postgres_target.execute(postgres_sql_text)
                conn_postgres_target.commit()
                print(f'track_{yyyymmdd} {((k / num_of_records) * 100):.3f}% Completed')

                if num_of_records - k <= 2:
                    # Calculate track duration and track distance at the end
                    postgres_sql_text = (
                        f"DROP TABLE IF EXISTS track_{yyyymmdd}{track_suffix}_temp_length; \n"
                        "SELECT *, end_time-start_time as track_duration, "
                        "ST_LengthSpheroid(geom, 'SPHEROID[\"WGS 84\",6378137,298.257223563]') / 1852 as track_length "
                        f"INTO track_{yyyymmdd}{track_suffix}_temp_length from track_{yyyymmdd}{track_suffix}_temp; "
                        f"DROP TABLE IF EXISTS track_{yyyymmdd}{track_suffix}_temp; "
                        f"ALTER TABLE track_{yyyymmdd}{track_suffix}_temp_length RENAME TO track_{yyyymmdd}{track_suffix}_temp;"
                    )

                    cursor_postgres_target.execute(postgres_sql_text)
                    conn_postgres_target.commit()
                    break

                k += 1

        postgres_sql_text = (
            f"DROP TABLE IF EXISTS track.track_{yyyymmdd}{track_suffix}_temp;\n"
            f"ALTER TABLE public.track_{yyyymmdd}{track_suffix}_temp SET SCHEMA track;"
        )
        cursor_postgres_target.execute(postgres_sql_text)
        conn_postgres_target.commit()

        postgres_sql_text = (
            f"DROP TABLE IF EXISTS track.track_cat62_{yyyymmdd}{track_suffix}; \n"
            f"SELECT track.track_{yyyymmdd}{track_suffix}_temp.geom, track.track_{yyyymmdd}{track_suffix}_temp.start_time, "
            f"track.track_{yyyymmdd}{track_suffix}_temp.end_time, track.track_{yyyymmdd}{track_suffix}_temp.track_duration, "
            f"track.track_{yyyymmdd}{track_suffix}_temp.track_length, flight_data.flight_{yyyymm}.* "
            f"INTO track.track_cat62_{yyyymmdd}{track_suffix} "
            f"FROM track.track_{yyyymmdd}{track_suffix}_temp "
            f"LEFT JOIN flight_data.flight_{yyyymm} ON "
            f"track.track_{yyyymmdd}{track_suffix}_temp.flight_key = flight_data.flight_{yyyymm}.flight_key;"
        )

        cursor_postgres_target.execute(postgres_sql_text)
        conn_postgres_target.commit()

        postgres_sql_text = (
            f"DROP TABLE IF EXISTS track.track_{yyyymmdd}{track_suffix}_temp; "
            f"GRANT SELECT ON ALL TABLES IN SCHEMA track TO public;"
        )
        cursor_postgres_target.execute(postgres_sql_text)
        conn_postgres_target.commit()

        # Add dest_rwy to the tracks
        print(f"working on adding arrival runway to {yyyymmdd} tracks")

        postgres_sql_text = (
            f"ALTER TABLE track.track_cat62_{yyyymmdd}{track_suffix} \n"
            f"DROP COLUMN IF EXISTS dest_rwy;\n"
            f"ALTER TABLE track.track_cat62_{yyyymmdd}{track_suffix} \n"
            f"ADD COLUMN dest_rwy character varying(3) DEFAULT '-';\n"
        )
        cursor_postgres_target.execute(postgres_sql_text)
        conn_postgres_target.commit()

        postgres_sql_text = (
            f"UPDATE track.track_cat62_{yyyymmdd}{track_suffix} t\n"
            f"SET dest_rwy = f.dest_rwy\n"
            f"FROM\n"
            f"(SELECT flight_key, dest_rwy\n"
            f"FROM\n"
            f"(SELECT flight_key, dest, dest_rwy, max(count)\n"
            f"FROM\n"
            f"(SELECT flight_key, dest, right(procedure_identifier, 2) as dest_rwy, COUNT(*)\n"
            f"FROM\n"
            f"(SELECT t.flight_key, t.position, t.vert, f.dest, b.airport_identifier, b.procedure_identifier\n"
            f"FROM sur_air.cat062_{yyyymmdd}{sur_air_suffix} t, \n"
            f"flight_data.flight_{yyyymm} f, \n"
            f"temp.vt_finalpath_buffer b\n"
            f"WHERE t.flight_id = f.id\n"
            f"AND (f.dest LIKE 'VT%')\n"
            f"AND ST_INTERSECTS(t.position, b.final_buffer)\n"
            f"UNION\n"
            f"SELECT t.flight_key, t.position, t.vert, f.dest, b.airport_identifier, b.procedure_identifier\n"
            f"FROM sur_air.cat062_{yyyymmdd_next}{sur_air_suffix} t, \n"
            f"flight_data.flight_{yyyymm} f, \n"
            f"temp.vt_finalpath_buffer b\n"
            f"WHERE t.flight_id = f.id\n"
            f"AND (f.dest LIKE 'VT%')\n"
            f"AND ST_INTERSECTS(t.position, b.final_buffer)\n"
            f") a\n"
            f"WHERE dest = airport_identifier AND vert = 2\n"
            f"AND length(procedure_identifier) = 3\n"
            f"GROUP BY flight_key, dest, procedure_identifier\n"
            f"UNION\n"
            f"SELECT flight_key, dest, right(procedure_identifier, 3) as dest_rwy, COUNT(*)\n"
            f"FROM\n"
            f"(SELECT t.flight_key, t.position, t.vert, f.dest, b.airport_identifier, b.procedure_identifier\n"
            f"FROM sur_air.cat062_{yyyymmdd}{sur_air_suffix} t, \n"
            f"flight_data.flight_{yyyymm} f, \n"
            f"temp.vt_finalpath_buffer b\n"
            f"WHERE t.flight_id = f.id\n"
            f"AND (f.dest LIKE 'VT%')\n"
            f"AND ST_INTERSECTS(t.position, b.final_buffer)\n"
            f"UNION\n"
            f"SELECT t.flight_key, t.position, t.vert, f.dest, b.airport_identifier, b.procedure_identifier\n"
            f"FROM sur_air.cat062_{yyyymmdd_next}{sur_air_suffix} t, \n"
            f"flight_data.flight_{yyyymm} f, \n"
            f"temp.vt_finalpath_buffer b\n"
            f"WHERE t.flight_id = f.id\n"
            f"AND (f.dest LIKE 'VT%')\n"
            f"AND ST_INTERSECTS(t.position, b.final_buffer)\n"
            f") a\n"
            f"WHERE dest = airport_identifier AND vert = 2\n"
            f"AND length(procedure_identifier) = 4\n"
            f"GROUP BY flight_key, dest, procedure_identifier) a\n"
            f"GROUP BY flight_key, dest, dest_rwy) b\n"
            f") f\n"
            f"WHERE t.flight_key = f.flight_key;\n"
        )
        cursor_postgres_target.execute(postgres_sql_text)
        conn_postgres_target.commit()

        # Add dep_rwy to the tracks
        print(f"working on adding departure runway to {yyyymmdd} tracks")

        postgres_sql_text = (
            f"ALTER TABLE track.track_cat62_{yyyymmdd}{track_suffix} \n"
            f"DROP COLUMN IF EXISTS dep_rwy;\n"
            f"ALTER TABLE track.track_cat62_{yyyymmdd}{track_suffix} \n"
            f"ADD COLUMN dep_rwy character varying(3) DEFAULT '-';\n"
        )
        cursor_postgres_target.execute(postgres_sql_text)
        conn_postgres_target.commit()

        postgres_sql_text = (
            f"UPDATE track.track_cat62_{yyyymmdd}{track_suffix} t\n"
            f"SET dep_rwy = f.dep_rwy\n"
            f"FROM\n"
            f"(SELECT flight_key, dep_rwy\n"
            f"FROM\n"
            f"(SELECT flight_key, dep, dep_rwy, max(count)\n"
            f"FROM\n"
            f"(SELECT flight_key, dep, right(runway_identifier, 2) as dep_rwy, COUNT(*)\n"
            f"FROM\n"
            f"(SELECT t.flight_key, t.position, t.vert, f.dep, b.airport_identifier, b.runway_identifier\n"
            f"FROM sur_air.cat062_{yyyymmdd}{sur_air_suffix} t, \n"
            f"flight_data.flight_{yyyymm} f, \n"
            f"temp.vt_dep_buffer b\n"
            f"WHERE t.flight_id = f.id\n"
            f"AND (f.dep LIKE 'VT%')\n"
            f"AND ST_INTERSECTS(t.position, b.buffer)\n"
            f") a\n"
            f"WHERE dep = airport_identifier AND vert = 1\n"
            f"AND length(runway_identifier) = 4\n"
            f"GROUP BY flight_key, dep, runway_identifier\n"
            f"UNION\n"
            f"SELECT flight_key, dep, right(runway_identifier, 3) as dep_rwy, COUNT(*)\n"
            f"FROM\n"
            f"(SELECT t.flight_key, t.position, t.vert, f.dep, b.airport_identifier, b.runway_identifier\n"
            f"FROM sur_air.cat062_{yyyymmdd}{sur_air_suffix} t, \n"
            f"flight_data.flight_{yyyymm} f, \n"
            f"temp.vt_dep_buffer b\n"
            f"WHERE t.flight_id = f.id\n"
            f"AND (f.dep LIKE 'VT%')\n"
            f"AND ST_INTERSECTS(t.position, b.buffer)\n"
            f") a\n"
            f"WHERE dep = airport_identifier AND vert = 1\n"
            f"AND length(runway_identifier) = 5\n"
            f"GROUP BY flight_key, dep, runway_identifier) a\n"
            f"GROUP BY flight_key, dep, dep_rwy) b\n"
            f") f\n"
            f"WHERE t.flight_key = f.flight_key;\n"
        )
        cursor_postgres_target.execute(postgres_sql_text)
        conn_postgres_target.commit()

        # Create entry_maintain_exit_fl table
        cursor_postgres_source = conn_postgres_source.cursor(cursor_factory=psycopg2.extras.DictCursor)
        postgres_sql_text = (
            f"DROP TABLE IF EXISTS track.entry_maintain_exit_fl_{yyyymmdd}; \n"
            f"CREATE TABLE track.entry_maintain_exit_fl_{yyyymmdd} ("
            "flight_key character varying, "
            "entry_fl double precision, "
            "maintain_fl double precision, "
            "exit_fl double precision) "
            "WITH (OIDS=FALSE);"
        )
        cursor_postgres_target.execute(postgres_sql_text)
        conn_postgres_target.commit()

        postgres_sql_text = (
            f"SELECT flight_key "
            f"FROM flight_data.flight_{yyyymm} "
            f"WHERE flight_key LIKE '%{year}-{month}-{day}%' "
            f"AND (mapped -> 'TopSky-ATC-MK-CAT062')::integer = 1 "
            f"ORDER BY flight_key ASC;"
        )

        cursor_postgres_source.execute(postgres_sql_text)
        flight_key_list = cursor_postgres_source.fetchall()
        print(flight_key_list)

        k = 0
        num_of_records = len(flight_key_list)

        for flight_key in flight_key_list:
            postgres_sql_text = (
                f"SELECT flight_key, measured_fl as entry_fl "
                f"FROM (SELECT * "
                f"FROM sur_air.cat062_{yyyymmdd}{sur_air_suffix} "
                f"UNION "
                f"SELECT * "
                f"FROM sur_air.cat062_{yyyymmdd_next}{sur_air_suffix}) x "
                f"WHERE flight_key = '{flight_key[0]}' "
                f"AND NOT sector IS NULL "
                f"ORDER BY app_time ASC "
                f"LIMIT 1"
            )
            cursor_postgres_source.execute(postgres_sql_text)
            record_1 = cursor_postgres_source.fetchall()

            postgres_sql_text = (
                f"SELECT flight_key, measured_fl as exit_fl "
                f"FROM (SELECT * "
                f"FROM sur_air.cat062_{yyyymmdd}{sur_air_suffix} "
                f"UNION "
                f"SELECT * "
                f"FROM sur_air.cat062_{yyyymmdd_next}{sur_air_suffix}) x "
                f"WHERE flight_key = '{flight_key[0]}' "
                f"AND NOT sector IS NULL "
                f"ORDER BY app_time DESC "
                f"LIMIT 1"
            )
            cursor_postgres_source.execute(postgres_sql_text)
            record_2 = cursor_postgres_source.fetchall()

            if record_1:
                temp_1 = record_1[0]
                temp_2 = record_2[0]

                entry_fl = none_to_null(str(temp_1['entry_fl']))
                exit_fl = none_to_null(str(temp_2['exit_fl']))

                postgres_sql_text = (
                    f"INSERT INTO track.entry_maintain_exit_fl_{yyyymmdd} ("
                    f"flight_key, entry_fl, exit_fl) "
                    f"VALUES ('{flight_key[0]}', {entry_fl}, {exit_fl});"
                )

                cursor_postgres_target.execute(postgres_sql_text)
                conn_postgres_target.commit()

                print(f'entry_maintain_exit_fl_{yyyymmdd} {((k / num_of_records) * 100):.3f}% Completed')

                k += 1

        # Add maintain_fl to the tracks
        cursor_postgres_source = conn_postgres_source.cursor(cursor_factory=psycopg2.extras.DictCursor)

        postgres_sql_text = (
            f"SELECT flight_key "
            f"FROM flight_data.flight_{yyyymm} "
            f"WHERE flight_key LIKE '%{year}-{month}-{day}%' "
            f"AND (mapped -> 'TopSky-ATC-MK-CAT062')::integer = 1 "
            f"ORDER BY flight_key ASC;"
        )
        cursor_postgres_source.execute(postgres_sql_text)
        flight_key_list = cursor_postgres_source.fetchall()

        k = 0
        num_of_records = len(flight_key_list)

        postgres_sql_text = (
            f"UPDATE track.entry_maintain_exit_fl_{yyyymmdd} t\n"
            f"SET maintain_fl = f.maintain_fl\n"
            f"FROM\n"
            f"(SELECT x.flight_key, x.measured_fl as maintain_fl\n"
            f"FROM\n"
            f"(SELECT flight_key, measured_fl, COUNT(*)\n"
            f"FROM\n"
            f"(SELECT app_time, flight_key, measured_fl\n"
            f"FROM sur_air.cat062_{yyyymmdd_next}{sur_air_suffix}\n"
            f"WHERE vert = 0 AND flight_key LIKE '%{year}-{month}-{day}%'\n"
            f"AND measured_fl > 100 AND NOT sector IS NULL\n"
            f"UNION\n"
            f"SELECT app_time, flight_key, measured_fl\n"
            f"FROM sur_air.cat062_{yyyymmdd}{sur_air_suffix}\n"
            f"WHERE vert = 0 AND flight_key LIKE '%{year}-{month}-{day}%'\n"
            f"AND measured_fl > 100 AND NOT sector IS NULL) a\n"
            f"GROUP BY flight_key, measured_fl) x,\n"
            f"(SELECT flight_key, max(count)\n"
            f"FROM\n"
            f"(SELECT flight_key, measured_fl, COUNT(*)\n"
            f"FROM\n"
            f"(SELECT app_time, flight_key, measured_fl\n"
            f"FROM sur_air.cat062_{yyyymmdd_next}{sur_air_suffix}\n"
            f"WHERE vert = 0 AND flight_key LIKE '%{year}-{month}-{day}%'\n"
            f"AND measured_fl > 100 AND NOT sector IS NULL\n"
            f"UNION\n"
            f"SELECT app_time, flight_key, measured_fl\n"
            f"FROM sur_air.cat062_{yyyymmdd}{sur_air_suffix}\n"
            f"WHERE vert = 0 AND flight_key LIKE '%{year}-{month}-{day}%'\n"
            f"AND measured_fl > 100 AND NOT sector IS NULL) a\n"
            f"GROUP BY flight_key, measured_fl\n"
            f"ORDER BY flight_key, count DESC) x\n"
            f"GROUP BY flight_key) y\n"
            f"WHERE x.flight_key = y.flight_key AND x.count = y.max\n"
            f"AND x.count > 12 * 1\n"
            f"ORDER BY flight_key) f\n"
            f"WHERE t.flight_key = f.flight_key\n"
        )
        cursor_postgres_target.execute(postgres_sql_text)
        conn_postgres_target.commit()

        postgres_sql_text = (
            f"ALTER TABLE track.track_cat62_{yyyymmdd}{track_suffix}\n"
            f"ADD COLUMN IF NOT EXISTS entry_fl double precision;\n"
            f"ALTER TABLE track.track_cat62_{yyyymmdd}{track_suffix}\n"
            f"ADD COLUMN IF NOT EXISTS maintain_fl double precision;\n"
            f"ALTER TABLE track.track_cat62_{yyyymmdd}{track_suffix}\n"
            f"ADD COLUMN IF NOT EXISTS exit_fl double precision;\n"
            f"UPDATE track.track_cat62_{yyyymmdd}{track_suffix} t\n"
            f"SET entry_fl = f.entry_fl FROM (SELECT * FROM track.entry_maintain_exit_fl_{yyyymmdd}) f\n"
            f"WHERE t.flight_key = f.flight_key;\n"
            f"UPDATE track.track_cat62_{yyyymmdd}{track_suffix} t\n"
            f"SET maintain_fl = f.maintain_fl FROM (SELECT * FROM track.entry_maintain_exit_fl_{yyyymmdd}) f\n"
            f"WHERE t.flight_key = f.flight_key;\n"
            f"UPDATE track.track_cat62_{yyyymmdd}{track_suffix} t\n"
            f"SET exit_fl = f.exit_fl FROM (SELECT * FROM track.entry_maintain_exit_fl_{yyyymmdd}) f\n"
            f"WHERE t.flight_key = f.flight_key;"
        )
        cursor_postgres_target.execute(postgres_sql_text)
        conn_postgres_target.commit()

        postgres_sql_text = f"DROP TABLE track.entry_maintain_exit_fl_{yyyymmdd};"
        cursor_postgres_target.execute(postgres_sql_text)
        conn_postgres_target.commit()

        command = (
            f"pg_dump --dbname=postgres://de_old_data:de_old_data@172.16.129.241:5432/aerothai_dwh "
            f"--table=track.track_cat62_{yyyymmdd} | psql -h localhost -W -U postgres temp"
        )
        print(command)
        p = Popen(command, shell=True, stdin=PIPE)
        p.communicate()
