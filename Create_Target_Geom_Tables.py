import psycopg2

# Try to connect to the local PostGresSQL database in which we will store our flight trajectories coupled with FPL data.
conn_postgres = psycopg2.connect(user = "postgres",
                                  password = "password",
                                  host = "127.0.0.1",
                                  port = "5432",
                                  database = "temp")
with conn_postgres:
    cursor_postgres = conn_postgres.cursor()
    year_list = ['2013']
    month_list = ['01','02','03','04','05','06','07','08','09','10','11','12']
    for year in year_list:
        for month in month_list:
            postgres_sql_text = f" DROP TABLE IF EXISTS target_{year}_{month}_geom; " \
                                f"SELECT a.position_id,a.callsign,a.app_time,a.src_time,a.speed,a.vx,a.vy," \
                                f"a.latitude,a.longitude,a.actual_flight_level,a.cdm, " \
                                f"a.dist_from_last_position,a.sector,a.nearby_fix,a.track_id,a.flight_id, " \
                                f"b.dep,b.dest,b.actype,b.reg,b.wturb,b.frule,b.op_type, " \
                                f"ST_SetSRID(ST_MakePoint(a.longitude,a.latitude),4326) as geom " \
                                f"INTO target_{year}_{month}_geom " \
                                f"FROM target_{year}_{month} a, \"{year}_{month}_fdmc\" b " \
                                f"where a.flight_id = b.flight_id"
            print(postgres_sql_text)
            cursor_postgres.execute(postgres_sql_text)
            conn_postgres.commit()