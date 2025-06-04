"""Process simtoolkit navigation data for VY region."""

from pathlib import Path
import psycopg2
import psycopg2.extras

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

AIRAC_LIST = ["2304"]
AIRAC_LIST = list(reversed(AIRAC_LIST))


def process_airac(airac: str) -> None:
    base_dir = Path(__file__).resolve().parent
    path_db = base_dir / "NavData" / f"simtoolkitpro_native_{airac}"

    sqllite_file_to_postgresql(DB_NAME, f"{path_db.as_posix()}/", SCHEMA_NAME)

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

    with psycopg2.connect(
        user="postgres",
        password="password",
        host="127.0.0.1",
        port="5432",
        database=DB_NAME,
    ) as conn_postgres:
        cur_pg = conn_postgres.cursor(cursor_factory=psycopg2.extras.DictCursor)
        postgres_sql_text = (
            f" CREATE SCHEMA IF NOT EXISTS {schema_name_2}_indonesia; "
            " SELECT tablename FROM pg_tables "
            f" WHERE schemaname = '{schema_name_2}' "
            " AND NOT(tablename like '%head%') "
            " AND NOT(tablename like 'sbas%');"
        )
        cur_pg.execute(postgres_sql_text)
        table_name_list = cur_pg.fetchall()
        for table_name in table_name_list:
            postgres_sql_text = (
                f" DROP TABLE IF EXISTS {schema_name_2}_indonesia.{table_name[0]};"
                f" SELECT * INTO {schema_name_2}_indonesia.{table_name[0]}"
                f" FROM {schema_name_2}.{table_name[0]}"
                f" WHERE public.ST_Intersects(geom,"
                f" (SELECT public.ST_Buffer(geom,2) FROM airspace.fir WHERE name like 'JAKARTA%') OR"
                f" public.ST_Intersects(geom,"
                f" (SELECT public.ST_Buffer(geom,2) FROM airspace.fir WHERE name like 'UJUNG%')));"
            )
            print(postgres_sql_text)
            cur_pg.execute(postgres_sql_text)
            conn_postgres.commit()

        postgres_sql_text = (
            f" DROP TABLE IF EXISTS {schema_name_2}_indonesia.tbl_header;"
            f" SELECT * INTO {schema_name_2}_indonesia.tbl_header"
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
