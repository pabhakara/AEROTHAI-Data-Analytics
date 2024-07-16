import csv
import psycopg2
from datetime import datetime, timedelta

# Database connection details
database = "aerothai_dwh"
username = "pongabhaab"
password = "pongabhaab2"
host = "172.16.129.241"
port = "5432"

# Date range
start_date = datetime(2023, 1, 1)
end_date = datetime(2023, 12, 31)

# Airport
airport = "VTBS"

# Geographical center point reference from bangkok tma polygon
# Query : SELECT st_centroid(geom) FROM airspace.tma WHERE name ='BANGKOK TMA'
longitude = 100.605555
latitude = 13.914797
radius_nm = 50

# # Geographical center point reference from bangkok tma polygon
# # Query : SELECT st_centroid(geom) FROM airspace.tma WHERE name ='BANGKOK TMA'
# longitude = 98.96277778
# latitude = 18.77138889
# radius_nm = 40

# Output file
output_file = f'{airport}_{start_date.strftime("%Y%m")}_{end_date.strftime("%Y%m")}.csv'

path = f'/Users/pongabha/Library/CloudStorage/Dropbox/Workspace/AEROTHAI Data Analytics/TMA Flight Efficiency/'

# Table query
sur_air = "sur_air.cat062"
track = "track.track_cat62"

# Connect to the PostgreSQL database
conn = psycopg2.connect(database=database, user=username, password=password, host=host, port=port)

# Create the CSV file and write the headers
with open(path+output_file, "w", newline="") as file:
    writer = csv.writer(file)
    writer.writerow(["callsign", "entry_time", "landed_time", "time_diff_minutes", "point_star", "rwy"])

    # Loop through the date range
    current_date = start_date
    while current_date <= end_date:
        # Construct table names and date format
        yyyymmdd = current_date.strftime("%Y%m%d")
        yyyymm = current_date.strftime("%Y%m")
        table_a = f"{sur_air}_{yyyymmdd}"
        table_b = f"{track}_{yyyymm}"

        print(f"Working on {yyyymmdd}")

        # Create the query
        query = f"""
        SELECT
            a.acid AS callsign,
            MIN(a.time_of_track) AS entry_time,
            b.aldt AS landed_time,
            EXTRACT(epoch FROM b.aldt - MIN(a.time_of_track)) / 60 AS time_diff_minutes,
            b.fdmc_bkk_fix[array_length(b.fdmc_bkk_fix,1)-1] AS point_star,
            b.dest_rwy AS rwy
        FROM
            {table_a} a
            LEFT JOIN {table_b} b ON a.flight_key = b.flight_key
        WHERE
            a.dest = '{airport}'
            AND ST_DWithin(
                a."position"::geography,
                ST_SetSRID(ST_MakePoint({longitude}, {latitude}), 4326)::geography,{radius_nm} * 1852
            )
            AND b.fdmc_bkk_fix[array_length(b.fdmc_bkk_fix,1)-1] IS NOT NULL
        GROUP BY
            a.acid, b.aldt, b.fdmc_bkk_fix[array_length(b.fdmc_bkk_fix,1)-1],b.dest_rwy
        HAVING 
            EXTRACT(epoch FROM b.aldt - MIN(a.time_of_track)) / 60 > 0
        ORDER BY
            entry_time ASC;
        """

        # Execute the query
        with conn.cursor() as cursor:
            cursor.execute(query)
            results = cursor.fetchall()

        # Write the results to the CSV file
        writer.writerows(results)

        # Move to the next date
        current_date += timedelta(days=1)

# Close the database connection
conn.close()