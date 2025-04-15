# cat062_etl_pipeline.py

import datetime as dt
import time
import pandas as pd
import psycopg2
import psycopg2.extras
from subprocess import PIPE, Popen
import os

def log_time(label):
    now = time.time()
    print(f"{label}: {dt.datetime.fromtimestamp(now).strftime('%Y-%m-%d %H:%M:%S')}")
    return now

def none_to_null(value):
    return 'null' if value == 'None' else f"'{value}'"

def connect_postgres(env: str, role: str):
    if env == 'remote':
        config = {
            "user": "de_old_data",
            "password": "de_old_data",
            "host": "172.16.129.241",
            "port": "5432",
            "database": "aerothai_dwh",
            "options": "-c search_path=dbo,public" if role == 'target' else "-c search_path=dbo,sur_air"
        }
    else:
        config = {
            "user": "postgres",
            "password": "password",
            "host": "127.0.0.1",
            "port": "5432",
            "database": "temp",
            "options": "-c search_path=dbo,public" if role == 'target' else "-c search_path=dbo,temp"
        }
    try:
        conn = psycopg2.connect(**config)
        print(f"{role.capitalize()} DB ({env}) connected successfully.")
        return conn
    except psycopg2.Error as e:
        print(f"Error connecting to {role} DB ({env}):", e)
        return None

def prepare_date_strings(date):
    year = f"{date.year}"
    month = f"{date.month:02d}"
    day = f"{date.day:02d}"
    yyyymmdd = f"{year}{month}{day}"
    yyyymm = f"{year}{month}"

    start_day = dt.datetime.strptime(f"{year}-{month}-{day}", '%Y-%m-%d')
    next_day = start_day + dt.timedelta(days=1)
    previous_day = start_day - dt.timedelta(days=1)

    yyyymmdd_next = next_day.strftime('%Y%m%d')
    yyyymmdd_previous = previous_day.strftime('%Y%m%d')

    return year, month, day, yyyymm, yyyymmdd, yyyymmdd_next, yyyymmdd_previous

def export_table_to_local(env, yyyymmdd):
    if env == 'remote':
        table_name = f"track.track_cat62_{yyyymmdd}"
        print(f"🔍 Checking if remote table {table_name} exists...")

        conn = connect_postgres(env, 'target')
        with conn.cursor() as cur:
            cur.execute(f"SELECT to_regclass('{table_name}');")
            if cur.fetchone()[0] is None:
                print(f"❌ Table {table_name} does not exist on remote.")
                return

        command = f"pg_dump --schema=track --table={table_name} --dbname=postgres://de_old_data:de_old_data@172.16.129.241:5432/aerothai_dwh " \
                  f"| psql -h localhost -W -U postgres temp"
        print(f"📤 Dumping {table_name} from remote to local...")
        p = Popen(command, shell=True, stdin=PIPE)
        p.communicate()

def create_track_table(cursor, yyyymmdd, suffix=""):
    sql = f"""
        DROP TABLE IF EXISTS track.track_{yyyymmdd}{suffix}_temp;
        CREATE TABLE track.track_{yyyymmdd}{suffix}_temp (
            acid character varying,
            track_no integer,
            geom geometry,
            start_time timestamp without time zone,
            end_time timestamp without time zone,
            icao_24bit_dap character varying,
            mode_a_code character varying,
            dep character varying,
            dest character varying,
            flight_id integer,
            flight_key character varying
        ) WITH (OIDS=FALSE);
    """
    cursor.execute(sql)

def insert_linestrings(cursor, linestrings, yyyymmdd, suffix=""):
    for entry in linestrings:
        sql = f"""
            INSERT INTO track.track_{yyyymmdd}{suffix}_temp (
                acid, track_no, icao_24bit_dap, mode_a_code, start_time,
                dep, dest, flight_key, flight_id, geom, end_time
            ) VALUES (
                %(acid)s, %(track_no)s, %(icao_24bit_dap)s, %(mode_a_code)s, %(start_time)s,
                %(dep)s, %(dest)s, %(flight_key)s, %(flight_id)s,
                ST_LineFromText(%(geom_wkt)s, 4326), %(end_time)s
            );
        """
        cursor.execute(sql, entry)

def extract_cat062_records(cursor, yyyymmdd, yyyymmdd_next, year, month, day, suffix=""):
    sql = f"""
        SELECT *
        FROM (
            SELECT track_no, time_of_track, icao_24bit_dap, mode_a_code, acid, acid_dap,
                   dep, dest, flight_key, flight_id, latitude, longitude, geo_alt
            FROM sur_air.cat062_{yyyymmdd}{suffix}
            WHERE latitude IS NOT NULL AND flight_id IS NOT NULL
              AND geo_alt >= 1 AND ground_speed BETWEEN 50 AND 700
              AND flight_key LIKE '%{year}-{month}-{day}%'

            UNION ALL

            SELECT track_no, time_of_track, icao_24bit_dap, mode_a_code, acid, acid_dap,
                   dep, dest, flight_key, flight_id, latitude, longitude, geo_alt
            FROM sur_air.cat062_{yyyymmdd_next}{suffix}
            WHERE latitude IS NOT NULL AND flight_id IS NOT NULL
              AND geo_alt >= 1 AND ground_speed BETWEEN 50 AND 700
              AND flight_key LIKE '%{year}-{month}-{day}%'
        ) a
        ORDER BY flight_key, time_of_track ASC;
    """
    cursor.execute(sql)
    return cursor.fetchall()

import datetime as dt

def is_valid_pair(rec1, rec2):
    return (
        rec1['track_no'] == rec2['track_no'] and
        abs(rec2['longitude'] - rec1['longitude']) < 1 and
        abs(rec2['latitude'] - rec1['latitude']) < 1 and
        (rec2['time_of_track'] - rec1['time_of_track']) <= dt.timedelta(minutes=1)
    )

def format_point(rec):
    lon = float(rec['longitude'])
    lat = float(rec['latitude'])
    alt = float(rec['geo_alt']) if rec['geo_alt'] is not None else -1
    return f"{lon} {lat} {alt}"

import math

def compute_bearing(p1, p2):
    """Compute the bearing (degrees) from p1 to p2."""
    lon1, lat1 = math.radians(p1['longitude']), math.radians(p1['latitude'])
    lon2, lat2 = math.radians(p2['longitude']), math.radians(p2['latitude'])
    dlon = lon2 - lon1
    x = math.sin(dlon) * math.cos(lat2)
    y = math.cos(lat1)*math.sin(lat2) - math.sin(lat1)*math.cos(lat2)*math.cos(dlon)
    return (math.degrees(math.atan2(x, y)) + 360) % 360

def should_skip_point(prev, curr, next_rec, heading_threshold=1, alt_threshold=50):
    """Return True if the current point can be skipped based on similar heading and altitude."""
    if not next_rec:
        return False

    prev_heading = compute_bearing(prev, curr)
    next_heading = compute_bearing(curr, next_rec)

    heading_diff = abs(prev_heading - next_heading)
    if heading_diff > 180:
        heading_diff = 360 - heading_diff

    alt_diff = abs(float(curr['geo_alt'] or -1) - float(prev['geo_alt'] or -1))
    next_alt_diff = abs(float(next_rec['geo_alt'] or -1) - float(curr['geo_alt'] or -1))

    return heading_diff < heading_threshold and alt_diff < alt_threshold and next_alt_diff < alt_threshold

def build_linestring_segment(track_records):
    """Construct a 3D LINESTRING WKT from a simplified sequence of track records and count skipped points."""
    simplified = []
    skipped_count = 0

    for i in range(len(track_records)):
        prev = track_records[i - 1] if i > 0 else None
        curr = track_records[i]
        next_rec = track_records[i + 1] if i < len(track_records) - 1 else None

        if prev and next_rec and should_skip_point(prev, curr, next_rec):
            skipped_count += 1
            continue

        simplified.append(format_point(curr))

    if len(simplified) == 1:
        simplified.append(simplified[0])

    return f"LINESTRING({','.join(simplified)})", skipped_count

def build_linestring_geometry(records, yyyymmdd, suffix=""):
    linestrings = []
    total_skipped = 0
    k = 0
    n = len(records)

    while k < n - 1:
        rec_start = records[k]
        rec_next = records[k + 1]

        if not is_valid_pair(rec_start, rec_next):
            k += 1
            continue

        segment = [rec_start]
        start_time = rec_start['time_of_track']

        while (
            k < n - 1 and
            records[k]['track_no'] == records[k + 1]['track_no'] and
            is_valid_pair(records[k], records[k + 1])
        ):
            segment.append(records[k + 1])
            k += 1

        end_time = segment[-1]['time_of_track']
        linestring_wkt, skipped = build_linestring_segment(segment)
        total_skipped += skipped

        entry = {
            'acid': segment[-1].get('acid'),
            'track_no': segment[-1]['track_no'],
            'icao_24bit_dap': segment[-1].get('icao_24bit_dap'),
            'mode_a_code': segment[-1].get('mode_a_code'),
            'start_time': start_time,
            'end_time': end_time,
            'dep': segment[-1].get('dep'),
            'dest': segment[-1].get('dest'),
            'flight_key': segment[-1].get('flight_key'),
            'flight_id': segment[-1].get('flight_id'),
            'geom_wkt': linestring_wkt
        }

        linestrings.append(entry)
        k += 1

    print(f"Total skipped points: {total_skipped}")
    return linestrings


def enrich_with_flight_data(cursor, yyyymm, yyyymmdd, suffix=""):
    sql = f"""
        DROP TABLE IF EXISTS track.track_cat62_{yyyymmdd}{suffix};
        SELECT 
            temp.geom, temp.start_time, temp.end_time,
            temp.end_time - temp.start_time AS track_duration,
            ST_LengthSpheroid(temp.geom, 'SPHEROID["WGS 84",6378137,298.257223563]') / 1852 AS track_length,
            f.*
        INTO track.track_cat62_{yyyymmdd}{suffix}
        FROM track.track_{yyyymmdd}{suffix}_temp temp
        LEFT JOIN flight_data.flight_{yyyymm} f
        ON temp.flight_key = f.flight_key;

        DROP TABLE IF EXISTS track.track_{yyyymmdd}{suffix}_temp;
        GRANT SELECT ON track.track_cat62_{yyyymmdd}{suffix} TO public;
    """
    cursor.execute(sql)
def add_runway_columns(cursor, yyyymmdd, suffix=""):
    sql = f"""
        ALTER TABLE track.track_cat62_{yyyymmdd}{suffix} 
        ADD COLUMN IF NOT EXISTS dep_rwy character varying(3) DEFAULT '-';

        ALTER TABLE track.track_cat62_{yyyymmdd}{suffix} 
        ADD COLUMN IF NOT EXISTS dest_rwy character varying(3) DEFAULT '-';
    """
    cursor.execute(sql)

def assign_arrival_runways(cursor, yyyymmdd, yyyymmdd_next, yyyymm, suffix=""):
    sql = f"""
        WITH all_cat062 AS (
            SELECT * FROM sur_air.cat062_{yyyymmdd}
            UNION ALL
            SELECT * FROM sur_air.cat062_{yyyymmdd_next}
        ),
        arr_candidates AS (
            SELECT t.flight_key,
                   CASE
                       WHEN LENGTH(b.procedure_identifier) = 4 THEN RIGHT(b.procedure_identifier, 3)
                       WHEN LENGTH(b.procedure_identifier) = 5 THEN RIGHT(LEFT(b.procedure_identifier, 3), 2)
                       ELSE RIGHT(b.procedure_identifier, 2)
                   END AS dest_rwy,
                   COUNT(*) AS cnt
            FROM all_cat062 t
            JOIN flight_data.flight_{yyyymm} f ON t.flight_id = f.id
            JOIN temp.vt_finalpath_buffer b ON ST_Intersects(t.position, b.final_buffer)
            WHERE f.dest = b.airport_identifier AND t.vert = 2 AND f.dest LIKE 'VT%'
            GROUP BY t.flight_key, b.procedure_identifier
        ),
        arr_selected AS (
            SELECT DISTINCT ON (flight_key) flight_key, dest_rwy
            FROM arr_candidates
            ORDER BY flight_key, cnt DESC
        )
        UPDATE track.track_cat62_{yyyymmdd}{suffix} t
        SET dest_rwy = a.dest_rwy
        FROM arr_selected a
        WHERE t.flight_key = a.flight_key;
    """
    cursor.execute(sql)

def assign_departure_runways(cursor, yyyymmdd, yyyymm, suffix=""):
    sql = f"""
        WITH dep_candidates AS (
            SELECT t.flight_key,
                   CASE 
                       WHEN LENGTH(b.runway_identifier) = 4 THEN RIGHT(b.runway_identifier, 2)
                       ELSE RIGHT(b.runway_identifier, 3)
                   END AS dep_rwy,
                   COUNT(*) AS cnt
            FROM sur_air.cat062_{yyyymmdd}{suffix} t
            JOIN flight_data.flight_{yyyymm} f ON t.flight_id = f.id
            JOIN temp.vt_dep_buffer b ON ST_Intersects(t.position, b.buffer)
            WHERE f.dep = b.airport_identifier AND t.vert = 1 AND f.dep LIKE 'VT%'
            GROUP BY t.flight_key, b.runway_identifier
        ),
        dep_selected AS (
            SELECT DISTINCT ON (flight_key) flight_key, dep_rwy
            FROM dep_candidates
            ORDER BY flight_key, cnt DESC
        )
        UPDATE track.track_cat62_{yyyymmdd}{suffix} t
        SET dep_rwy = d.dep_rwy
        FROM dep_selected d
        WHERE t.flight_key = d.flight_key;
    """
    cursor.execute(sql)
def assign_runways(cursor, yyyymmdd, yyyymm, yyyymmdd_next, suffix=""):
    print(f"Assigning runways for {yyyymmdd}.")

    add_runway_columns(cursor, yyyymmdd, suffix)
    assign_arrival_runways(cursor, yyyymmdd, yyyymmdd_next, yyyymm, suffix)
    assign_departure_runways(cursor, yyyymmdd, yyyymm, suffix)

    print(f"Runway assignment completed for {yyyymmdd}.")

def add_flight_level_columns(cursor, yyyymmdd, suffix=""):
    sql = f"""
        ALTER TABLE track.track_cat62_{yyyymmdd}{suffix}
        ADD COLUMN IF NOT EXISTS entry_fl double precision;

        ALTER TABLE track.track_cat62_{yyyymmdd}{suffix}
        ADD COLUMN IF NOT EXISTS maintain_fl double precision;

        ALTER TABLE track.track_cat62_{yyyymmdd}{suffix}
        ADD COLUMN IF NOT EXISTS exit_fl double precision;
    """
    cursor.execute(sql)

def assign_entry_flight_level(cursor, yyyymmdd, yyyymmdd_next, year, month, day, suffix=""):
    sql = f"""
        WITH cat062_union AS (
            SELECT * FROM sur_air.cat062_{yyyymmdd}{suffix}
            UNION
            SELECT * FROM sur_air.cat062_{yyyymmdd_next}{suffix}
        ),
        entry_fl_cte AS (
            SELECT flight_key, measured_fl AS entry_fl
            FROM cat062_union
            WHERE sector IS NOT NULL AND flight_key LIKE '%{year}-{month}-{day}%'
            ORDER BY app_time ASC
        )
        UPDATE track.track_cat62_{yyyymmdd}{suffix} t
        SET entry_fl = f.entry_fl
        FROM entry_fl_cte f
        WHERE t.flight_key = f.flight_key;
    """
    cursor.execute(sql)

def assign_exit_flight_level(cursor, yyyymmdd, yyyymmdd_next, year, month, day, suffix=""):
    sql = f"""
        WITH cat062_union AS (
            SELECT * FROM sur_air.cat062_{yyyymmdd}{suffix}
            UNION
            SELECT * FROM sur_air.cat062_{yyyymmdd_next}{suffix}
        ),
        exit_fl_cte AS (
            SELECT flight_key, measured_fl AS exit_fl
            FROM cat062_union
            WHERE sector IS NOT NULL AND flight_key LIKE '%{year}-{month}-{day}%'
            ORDER BY app_time DESC
        )
        UPDATE track.track_cat62_{yyyymmdd}{suffix} t
        SET exit_fl = f.exit_fl
        FROM exit_fl_cte f
        WHERE t.flight_key = f.flight_key;
    """
    cursor.execute(sql)


def assign_maintain_flight_level(cursor, yyyymmdd, yyyymmdd_next, year, month, day, suffix=""):
    sql = f"""
        WITH cat062_union AS (
            SELECT app_time, flight_key, measured_fl
            FROM sur_air.cat062_{yyyymmdd}{suffix}
            WHERE vert = 0 AND measured_fl > 100 AND sector IS NOT NULL AND flight_key LIKE '%{year}-{month}-{day}%'
            UNION ALL
            SELECT app_time, flight_key, measured_fl
            FROM sur_air.cat062_{yyyymmdd_next}{suffix}
            WHERE vert = 0 AND measured_fl > 100 AND sector IS NOT NULL AND flight_key LIKE '%{year}-{month}-{day}%'
        ),
        fl_counts AS (
            SELECT flight_key, measured_fl, COUNT(*) AS cnt
            FROM cat062_union
            GROUP BY flight_key, measured_fl
        ),
        fl_max AS (
            SELECT flight_key, MAX(cnt) AS max_cnt
            FROM fl_counts
            GROUP BY flight_key
        ),
        maintain_cte AS (
            SELECT c.flight_key, c.measured_fl AS maintain_fl
            FROM fl_counts c
            JOIN fl_max m ON c.flight_key = m.flight_key AND c.cnt = m.max_cnt
        )
        UPDATE track.track_cat62_{yyyymmdd}{suffix} t
        SET maintain_fl = f.maintain_fl
        FROM maintain_cte f
        WHERE t.flight_key = f.flight_key;
    """
    cursor.execute(sql)

def assign_flight_levels(cursor, yyyymmdd, yyyymmdd_next, yyyymm, year, month, day, suffix=""):
    print(f"Assigning entry, maintain, and exit flight levels for {yyyymmdd}...")

    add_flight_level_columns(cursor, yyyymmdd, suffix)
    assign_entry_flight_level(cursor, yyyymmdd, yyyymmdd_next, year, month, day, suffix)
    assign_exit_flight_level(cursor, yyyymmdd, yyyymmdd_next, year, month, day, suffix)
    assign_maintain_flight_level(cursor, yyyymmdd, yyyymmdd_next, year, month, day, suffix)

    print(f"Flight level assignment completed for {yyyymmdd}.")

def import_track_from_remote(yyyymmdd):
    remote_pg_uri = "postgres://de_old_data:de_old_data@172.16.129.241:5432/aerothai_dwh"
    local_pg_user = "postgres"
    local_pg_db = "temp"
    local_pg_password = "password"
    local_pg_host = "localhost"
    local_pg_port = "5432"
    schema = "track"
    table = f"track_cat62_{yyyymmdd}"
    full_table = f"{schema}.{table}"
    print(f"⬇️  Importing {full_table} from remote...")
    command = (
        f"pg_dump --dbname={remote_pg_uri} "
        f"--table={full_table} | "
        f"psql -h {local_pg_host} -U {local_pg_user} -d {local_pg_db}"
    )
    env = os.environ.copy()
    env["PGPASSWORD"] = local_pg_password

    p = Popen(command, shell=True, env=env, stdin=PIPE, stdout=PIPE, stderr=PIPE)
    stdout, stderr = p.communicate()

    if p.returncode == 0:
        print(f"✅ Successfully imported {full_table}")
    else:
        print(f"❌ Failed to import {full_table}")
        print("STDERR:\n", stderr.decode())


def process_day(date, target_conn, source_conn, environment='remote', suffix=''):
    year, month, day, yyyymm, yyyymmdd, yyyymmdd_next, _ = prepare_date_strings(date)

    with target_conn:
        cursor_target = target_conn.cursor()
        create_track_table(cursor_target, yyyymmdd, suffix)

        with source_conn:
            cursor_source = source_conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
            records = extract_cat062_records(cursor_source, yyyymmdd, yyyymmdd_next, year, month, day, suffix)

        lines = build_linestring_geometry(records, yyyymmdd, suffix)
        insert_linestrings(cursor_target, lines, yyyymmdd, suffix)
        enrich_with_flight_data(cursor_target, yyyymm, yyyymmdd, suffix)
        assign_runways(cursor_target, yyyymmdd, yyyymm, yyyymmdd_next, suffix)
        assign_flight_levels(cursor_target, yyyymmdd, yyyymmdd_next, yyyymm, year, month, day, suffix)
        if environment == 'remote':
            import_track_from_remote(yyyymmdd)

        # remote_pg_uri = "postgres://de_old_data:de_old_data@172.16.129.241:5432/aerothai_dwh"
        # local_pg_user = "postgres"
        # local_pg_db = "temp"
        # local_pg_password = "password"
        # local_pg_host = "localhost"
        # local_pg_port = "5432"
        # schema = "track"
        # table = f"track_cat62_{yyyymmdd}"
        # full_table = f"{schema}.{table}"
        # print(f"⬇️  Importing {full_table} from remote...")
        # command = (
        #     f"pg_dump --dbname={remote_pg_uri} "
        #     f"--table={full_table} | "
        #     f"psql -h {local_pg_host} -U {local_pg_user} -d {local_pg_db}"
        # )
        # env = os.environ.copy()
        # env["PGPASSWORD"] = local_pg_password
        #
        # p = Popen(command, shell=True, env=env, stdin=PIPE, stdout=PIPE, stderr=PIPE)
        # stdout, stderr = p.communicate()
        #
        # if p.returncode == 0:
        #     print(f"✅ Successfully imported {full_table}")
        # else:
        #     print(f"❌ Failed to import {full_table}")
        #     print("STDERR:\n", stderr.decode())


if __name__ == "__main__":
    start_time = log_time("Start time")

    ENVIRONMENT = 'remote'

    # date_list = pd.date_range(start='2023-04-09', end='2023-04-10')

    today = dt.datetime.now()
    date_list = [dt.datetime.strptime(f"{today.year}-{today.month}-{today.day}", '%Y-%m-%d')
                 + dt.timedelta(days=-73)]

    conn_postgres_target = connect_postgres(ENVIRONMENT, 'target')
    conn_postgres_source = connect_postgres(ENVIRONMENT, 'source')

    for date in date_list:
        print(f"\n🛫 Processing {date.strftime('%Y-%m-%d')}...")
        process_day(date, conn_postgres_target, conn_postgres_source, ENVIRONMENT)

    end_time = log_time("End time")
    elapsed = end_time - start_time
    print(f"\n⏱️ Total execution time: {elapsed:.2f} seconds")
