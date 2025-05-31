import os
import psycopg2
import datetime as dt
import pandas as pd


def get_connection():
    """Return a Postgres connection using environment variables when available."""
    return psycopg2.connect(
        user=os.getenv("DB_USER", "de_old_data"),
        password=os.getenv("DB_PASSWORD", "de_old_data"),
        host=os.getenv("DB_HOST", "172.16.129.241"),
        port=os.getenv("DB_PORT", "5432"),
        database=os.getenv("DB_NAME", "aerothai_dwh"),
        options="-c search_path=dbo,los",
    )


def build_sql(year: str, month: str, day: str) -> str:
    """Return the SQL text used for creating LOS tables."""
    return f"""
    DROP TABLE IF EXISTS los.los_{year}_{month}_{day};
    SELECT a.app_time,
           a.flight_key AS flight_key_a,
           b.flight_key AS flight_key_b,
           a.flight_id AS flight_id_a,
           b.flight_id AS flight_id_b,
           public.ST_Distance(
               public.ST_Transform(a."position", 3857),
               public.ST_Transform(b."position", 3857)
           ) / 1852 AS horizontal_separation,
           abs(a.measured_fl - b.measured_fl) AS vertical_separation,
           public.ST_MakeLine(a."position", b."position") AS "geom",
           a.measured_fl AS fl_a,
           b.measured_fl AS fl_b,
           a.ground_speed AS ground_speed_a,
           b.ground_speed AS ground_speed_b,
           a.vx AS vx_a,
           a.vy AS vy_a,
           b.vx AS vx_b,
           b.vy AS vy_b,
           a.dist_from_last_position AS dist_from_last_position_a,
           b.dist_from_last_position AS dist_from_last_position_b
    INTO los.los_{year}_{month}_{day}
    FROM sur_air.cat062_{year}{month}{day} a,
         sur_air.cat062_{year}{month}{day} b
    WHERE NOT (a.flight_key = b.flight_key)
      AND a.app_time = b.app_time
      AND abs(a.measured_fl - b.measured_fl) < 9.75
      AND (a.measured_fl > 1 AND b.measured_fl > 10)
      AND public.ST_Distance(
            public.ST_Transform(a."position", 3857),
            public.ST_Transform(b."position", 3857)
          ) / 1852 < 5
    ORDER BY flight_key_a, flight_key_b, app_time;

    DROP TABLE IF EXISTS los.los_{year}_{month}_{day}_temp;
    SELECT los.los_{year}_{month}_{day}.*,
           a.acid AS callsign_a,
           b.acid AS callsign_b,
           a.actype AS actype_a,
           b.actype AS actype_b,
           a.dep AS adep_a,
           a.dest AS ades_a,
           b.dep AS adep_b,
           b.dest AS ades_b,
           a.op_type AS op_type_a,
           b.op_type AS op_type_b,
           a.frule AS frule_a,
           b.frule AS frule_b
    INTO los.los_{year}_{month}_{day}_temp
    FROM los.los_{year}_{month}_{day}
    LEFT JOIN flight_data.flight_{year}{month} a
           ON los.los_{year}_{month}_{day}.flight_id_a = a.id
    LEFT JOIN flight_data.flight_{year}{month} b
           ON los.los_{year}_{month}_{day}.flight_id_b = b.id;

    DROP TABLE IF EXISTS los.los_{year}_{month}_{day};
    ALTER TABLE los.los_{year}_{month}_{day}_temp
        RENAME TO los_{year}_{month}_{day};
    DELETE FROM los_{year}_{month}_{day}
    WHERE frule_a = 'V' AND frule_b = 'V';
    ALTER TABLE los.los_{year}_{month}_{day}
        DROP COLUMN IF EXISTS time_of_los;
    DROP TABLE IF EXISTS los.los_{year}_{month}_{day}_temp;
    SELECT * INTO los.los_{year}_{month}_{day}_temp
    FROM (
        SELECT a.app_time - b.time_diff AS time_of_los, a.*
        FROM los.los_{year}_{month}_{day} a,
             (SELECT MIN(app_time - time_of_track) AS time_diff
              FROM sur_air.cat062_{year}{month}{day}) b
    ) a;
    DROP TABLE los.los_{year}_{month}_{day};
    ALTER TABLE los.los_{year}_{month}_{day}_temp
        RENAME TO los_{year}_{month}_{day};
    GRANT USAGE ON SCHEMA los TO ponkritsa;
    GRANT SELECT ON ALL TABLES IN SCHEMA los TO ponkritsa;
    GRANT USAGE ON SCHEMA los TO saifonob;
    GRANT SELECT ON ALL TABLES IN SCHEMA los TO saifonob;
    GRANT USAGE ON SCHEMA los TO ponlawatwa;
    GRANT SELECT ON ALL TABLES IN SCHEMA los TO ponlawatwa;
    GRANT USAGE ON SCHEMA los TO pongabhaab;
    GRANT SELECT ON ALL TABLES IN SCHEMA los TO pongabhaab;
    GRANT USAGE ON SCHEMA los TO de_old_data;
    GRANT SELECT ON ALL TABLES IN SCHEMA los TO de_old_data;
    """


def main():
    today = dt.datetime.now()
    date_list = [
        dt.datetime.strptime(f"{today.year}-{today.month}-{today.day}", "%Y-%m-%d")
        + dt.timedelta(days=-3)
    ]

    with get_connection() as conn:
        with conn.cursor() as cur:
            for date in date_list:
                year = f"{date.year}"
                month = f"{date.month:02d}"
                day = f"{date.day:02d}"
                sql = build_sql(year, month, day)
                print(f"working on {year}{month}{day} los")
                cur.execute(sql)
                conn.commit()


if __name__ == "__main__":
    main()
