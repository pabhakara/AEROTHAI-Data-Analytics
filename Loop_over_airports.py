import psycopg2
import psycopg2.extras
import time
import datetime as dt
import pandas as pd

# Create a connection to the remote PostGresSQL database from which we will retrieve our data for processing in Python

conn_postgres_source = psycopg2.connect(user = "de_old_data",
                                  password = "de_old_data",
                                  host = "172.16.129.241",
                                  port = "5432",
                                  database = "aerothai_dwh",
                                  options="-c search_path=dbo,public")

# conn_postgres_source = psycopg2.connect(user = "postgres",
#                                   password = "password",
#                                   host = "127.0.0.1",
#                                   port = "5432",
#                                   database = "temp")
#                                   #options="-c search_path=dbo,public")

start_date = '2023-06-01'
end_date = '2023-06-18'

date_list = pd.date_range(start=start_date, end=end_date)

airport_identifier_list = ['VTBS','VTBD','VTSP','VTCC','VTSM']

summary_df = pd.DataFrame()

with conn_postgres_source:
    cursor_postgres_source = conn_postgres_source.cursor(cursor_factory=psycopg2.extras.DictCursor)
    for date in date_list:
        year = f"{date.year}"
        month = f"{date.month:02d}"
        day = f"{date.day:02d}"
        yyyymmdd = f"{year}{month}{day:}"
        yyyymm = f"{year}{month}"

        print(f"Working on {yyyymmdd}")
        # Create an SQL query that selects surveillance targets from the source PostgreSQL database
        for airport_identifier in airport_identifier_list:
            postgres_sql_text = f"SELECT c.airport_identifier,c.waypoint_identifier,c.altitude1 as alt_constraint, " \
                                f"b.acid,b.actype,a.time_of_track,a.ias_dap,a.tas_dap, " \
                                f"ROUND(a.ground_speed::numeric,2) as ground_speed,a.measured_fl " \
                                f"FROM  " \
                                f"sur_air.cat062_{yyyymmdd} a, " \
                                f"flight_data.flight_{yyyymm} b, " \
                                f"airac_current.iaps_wp c " \
                                f"WHERE b.dest LIKE '{airport_identifier}' " \
                                f"AND c.airport_identifier LIKE '{airport_identifier}' " \
                                f"AND c.procedure_identifier LIKE 'R%' " \
                                f"AND c.waypoint_description_code LIKE '%I%' " \
                                f"AND ST_DWithin(a.position,c.geom::geography,1 * 1852) " \
                                f"AND b.flight_key = a.flight_key " \
                                f"AND NOT a.ias_dap IS NULL " \
                                f"ORDER BY acid,time_of_track ASC"

            cursor_postgres_source.execute(postgres_sql_text)
            # print(postgres_sql_text)

            record = cursor_postgres_source.fetchall()
            column_list = ['airport', 'waypoint', 'alt_constraint', 'acid', 'actype', 'time','ias','tas','gnd_speed','fl']
            df_temp = pd.DataFrame(record, columns=column_list)
            summary_df = pd.concat([summary_df, df_temp])
print(summary_df)
path = f"/Users/pongabha/Library/CloudStorage/Dropbox/Workspace/AEROTHAI Data Analytics/"
filename = f"aircraft_speed_at_if_{start_date}_to_{end_date}.csv"
summary_df.to_csv(f"{path}{filename}",index=False)

