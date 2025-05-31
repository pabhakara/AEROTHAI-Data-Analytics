import os
import psycopg2
import time


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


def build_sql(year: str, month: str) -> str:
    year_month = f"{year}_{month}"
    return f"""
    DROP TABLE IF EXISTS los.los_{year_month};
    SELECT a.app_time,
           a.callsign AS callsign_a,
           b.callsign AS callsign_b,
           a.actype AS actype_a,
           b.actype AS actype_b,
           a.dep AS adep_a,
           a.dest AS ades_a,
           b.dep AS adep_b,
           b.dest AS ades_b,
           a.op_type AS op_type_a,
           b.op_type AS op_type_b,
           a.frule AS frule_a,
           b.frule AS frule_b,
           public.ST_Distance(
               public.ST_Transform(a.geom, 3857),
               public.ST_Transform(b.geom, 3857)
           ) / 1852 AS horizontal_separation,
           abs(a.actual_flight_level - b.actual_flight_level) AS vertical_separation,
           public.ST_MakeLine(a.geom, b.geom) AS geom,
           a.actual_flight_level AS fl_a,
           b.actual_flight_level AS fl_b,
           a.cdm AS cdm_a,
           b.cdm AS cdm_b,
           a.speed AS speed_a,
           b.speed AS speed_b,
           a.vx AS vx_a,
           a.vy AS vy_a,
           b.vx AS vx_b,
           b.vy AS vy_b,
           a.dist_from_last_position AS dist_from_last_position_a,
           b.dist_from_last_position AS dist_from_last_position_b
    INTO los.los_{year_month}
    FROM sur_air.target_{year_month}_geom a,
         sur_air.target_{year_month}_geom b
    WHERE a.app_time = b.app_time
      AND abs(a.actual_flight_level - b.actual_flight_level) < 10
      AND (a.actual_flight_level > 1 AND b.actual_flight_level > 1)
      AND (a.frule = 'I' OR b.frule = 'I')
      AND public.ST_Distance(
            public.ST_Transform(a.geom, 3857),
            public.ST_Transform(b.geom, 3857)
          ) / 1852 < 8
      AND NOT (a.flight_id = b.flight_id)
      AND NOT (a.callsign = b.callsign)
    ORDER BY app_time, horizontal_separation, vertical_separation ASC;

    DELETE FROM los.los_{year_month} a
    USING (
        SELECT MIN(callsign_a) AS callsign_a,
               app_time,
               horizontal_separation
        FROM los.los_{year_month}
        GROUP BY app_time, horizontal_separation
        HAVING COUNT(*) > 1
    ) b
    WHERE (a.app_time = b.app_time)
      AND (a.horizontal_separation = b.horizontal_separation)
      AND a.callsign_a <> b.callsign_a;
    """


def main():
    year_list = ["2020", "2019", "2018", "2017", "2016", "2015", "2014", "2013"]
    month_list = ["01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11", "12"]

    with get_connection() as conn:
        with conn.cursor() as cur:
            for year in year_list:
                for month in month_list:
                    sql = build_sql(year, month)
                    print(f"Processing {year}_{month}")
                    cur.execute(sql)
                    conn.commit()


if __name__ == "__main__":
    main()
