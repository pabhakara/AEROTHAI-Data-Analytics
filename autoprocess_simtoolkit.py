"""Utility script for importing simtoolkit navigation data into PostgreSQL."""

from pathlib import Path

from Create_MORA_Grid_simtoolkit import create_mora_grid
from Create_SID_Legs_simtoolkit import create_sid_legs
from Create_SID_Legs_RF_simtoolkit import create_sid_legs_rf
from Create_SID_Legs_simtoolkit_without_RF import create_sid_legs_without_rf
from Create_STAR_Legs_simtoolkit import create_star_legs
from Create_STAR_Legs_RF_simtoolkit import create_star_legs_rf
from Create_STAR_Legs_simtoolkit_without_RF import create_star_legs_without_rf
from Create_IAP_Legs_RF_simtoolkit import create_iap_legs_rf
from Create_IAP_Legs_AF_simtoolkit import create_iap_legs_af
from Create_IAP_Legs_simtoolkit import create_iap_legs
from Create_ATS_Route_Segments_simtoolkit import create_ats_route_segments
from Create_ATS_Route_simtoolkit import create_ats_route
from Create_Runway_Segments_simtoolkit import create_runway_segments
from Create_Holding_Legs import create_holding_legs
from Create_Holding_Legs_from_IAPs import create_holding_legs_from_iaps
from SQLite_File_to_PostgreSQL import sqllite_file_to_postgresql

import psycopg2


def tic() -> None:
    """Start timer."""
    import time

    global startTime_for_tictoc
    startTime_for_tictoc = time.time()

def toc() -> None:
    """Print elapsed time since :func:`tic`."""
    import time

    if 'startTime_for_tictoc' in globals():
        print(f"Elapsed time is {time.time() - startTime_for_tictoc} seconds.")
    else:
        print("Toc: start time not set")


DB_NAME = "navigraph"
SCHEMA_NAME = "public"

AIRAC_LIST = ["2503"]  # Add more AIRAC cycles as needed


def process_airac(airac: str) -> None:
    """Load navigation data for a given AIRAC cycle."""

    base_dir = Path(__file__).resolve().parent
    path_db = base_dir / "NavData" / f"simtoolkitpro_native_{airac}"

    # Populate the database with simtoolkit navdata from the sqlite file
    sqllite_file_to_postgresql(DB_NAME, f"{path_db.as_posix()}/", SCHEMA_NAME)

    # Create waypoints and clean runway table
    with psycopg2.connect(
        user="postgres",
        password="password",
        host="127.0.0.1",
        port="5432",
        database=DB_NAME,
        options="-c search_path=dbo," + SCHEMA_NAME,
    ) as conn2:
        with open(base_dir / "create_wp.sql", "r") as sql_file:
            cur2 = conn2.cursor()
            cur2.execute(sql_file.read())
        cur2.execute(
            "DELETE FROM public.tbl_runways WHERE runway_true_bearing IS NULL;"
        )
        conn2.commit()

    # Create navigation tables
    create_mora_grid(DB_NAME, SCHEMA_NAME)
    create_sid_legs(DB_NAME, SCHEMA_NAME)
    create_sid_legs_rf(DB_NAME, SCHEMA_NAME)
    create_sid_legs_without_rf(DB_NAME, SCHEMA_NAME)
    create_star_legs(DB_NAME, SCHEMA_NAME)
    create_star_legs_rf(DB_NAME, SCHEMA_NAME)
    create_star_legs_without_rf(DB_NAME, SCHEMA_NAME)
    create_iap_legs_rf(DB_NAME, SCHEMA_NAME)
    create_iap_legs_af(DB_NAME, SCHEMA_NAME)
    create_iap_legs(DB_NAME, SCHEMA_NAME)
    create_ats_route_segments(DB_NAME, SCHEMA_NAME)
    create_ats_route(DB_NAME, SCHEMA_NAME)
    create_runway_segments(DB_NAME, SCHEMA_NAME)
    create_holding_legs(DB_NAME, SCHEMA_NAME)
    create_holding_legs_from_iaps(DB_NAME, SCHEMA_NAME)

    # Clean up temporary tables
    with psycopg2.connect(
        database=DB_NAME,
        user="postgres",
        password="password",
        host="127.0.0.1",
        port="5432",
    ) as conn3:
        cur3 = conn3.cursor()
        with open(base_dir / "clean_up_legs.sql", "r") as sql_file:
            cur3.execute(sql_file.read())

        schema_name_2 = f"airac_{airac}"
        print(schema_name_2)
        postgres_sql_text = (
            f"DROP SCHEMA IF EXISTS {schema_name_2} CASCADE;"
            f"CREATE SCHEMA {schema_name_2};"
            "DO "
            "$$ "
            "DECLARE "
            "row record; "
            "BEGIN "
            "FOR row IN SELECT tablename FROM pg_tables "
            "WHERE schemaname = 'public' and NOT(tablename like 'spat%') "
            "LOOP "
            f"EXECUTE 'DROP TABLE IF EXISTS {schema_name_2}.' || quote_ident(row.tablename) || ' ;'; "
            f"EXECUTE 'ALTER TABLE public.' || quote_ident(row.tablename) || ' SET SCHEMA {schema_name_2};'; "
            " END LOOP; "
            "END; "
            "$$;"
        )
        cur3.execute(postgres_sql_text)
        conn3.commit()

    # Create VT version of NavData
    with psycopg2.connect(
        user="postgres",
        password="password",
        host="127.0.0.1",
        port="5432",
        database=DB_NAME,
    ) as conn_postgres:
        cur_pg = conn_postgres.cursor(cursor_factory=psycopg2.extras.DictCursor)
        postgres_sql_text = (
            f" CREATE SCHEMA IF NOT EXISTS {schema_name_2}_vt; "
            " SELECT tablename FROM pg_tables "
            f" WHERE schemaname = '{schema_name_2}' "
            " AND NOT(tablename like '%head%') "
            " AND NOT(tablename like 'sbas%');"
        )
        cur_pg.execute(postgres_sql_text)
        table_name_list = cur_pg.fetchall()
        for table_name in table_name_list:
            postgres_sql_text = (
                f" DROP TABLE IF EXISTS {schema_name_2}_vt.{table_name[0]};"
                f" SELECT * INTO {schema_name_2}_vt.{table_name[0]}"
                f" FROM {schema_name_2}.{table_name[0]}"
                f" WHERE public.ST_Intersects(geom,"
                f" (SELECT public.ST_Buffer(geom,10) FROM airspace.fir WHERE name like 'BANGKOK%'));"
            )
            print(postgres_sql_text)
            cur_pg.execute(postgres_sql_text)
            conn_postgres.commit()

        postgres_sql_text = (
            f" DROP TABLE IF EXISTS {schema_name_2}_vt.tbl_header;"
            f" SELECT * INTO {schema_name_2}_vt.tbl_header"
            f" FROM {schema_name_2}.tbl_header;"
        )
        cur_pg.execute(postgres_sql_text)
        conn_postgres.commit()


def main() -> None:
    tic()
    for airac in AIRAC_LIST:
        process_airac(airac)
    toc()


if __name__ == "__main__":
    main()
