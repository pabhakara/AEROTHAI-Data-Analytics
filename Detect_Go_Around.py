import pandas as pd
import psycopg2
from datetime import datetime, timedelta

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


# === Parameters ===
ALT_NEAR_GROUND = 3000  # feet
CLIMB_THRESHOLD = 500  # feet
DESCENT_WINDOW = 3  # samples
START_DATE = datetime(2025, 1, 1)
END_DATE = datetime(2025, 1, 31)  # inclusive


# === Detection function ===
def detect_go_around(track_df):
    track_df = track_df.reset_index(drop=True)
    for i in range(DESCENT_WINDOW, len(track_df) - 5):
        descending = track_df.loc[i - DESCENT_WINDOW:i, "alt_diff"] < 0
        near_ground = track_df.loc[i, "altitude_ft"] < ALT_NEAR_GROUND
        climbing = track_df.loc[i + 1:i + 3, "alt_diff"] > 0
        climb_gain = track_df.loc[i + 3:i + 6, "altitude_ft"].max() - track_df.loc[i, "altitude_ft"]
        if descending.all() and near_ground and climbing.any() and climb_gain > CLIMB_THRESHOLD:
            return True
    return False


# === Results storage ===
go_around_events = []

# === Main loop over each day ===
current_date = START_DATE
while current_date <= END_DATE:
    table_date_str = current_date.strftime('%Y%m%d')
    table_name = f"sur_air.cat062_{table_date_str}"

    print(f"Processing table: {table_name}")

    # Step 1: Load daily radar data
    try:
        sql_query = f"""
        SELECT
            flight_key,
            time_of_track,
            measured_fl,
            latitude,
            longitude,
            ground_speed
        FROM {table_name}
        WHERE measured_fl IS NOT NULL AND measured_fl < 50
        ORDER BY flight_key, time_of_track;
        """
        df = pd.read_sql(sql_query, conn_postgres_target)
        if df.empty:
            print(f"No data on {table_date_str}")
            current_date += timedelta(days=1)
            continue
    except Exception as e:
        print(f"Error reading {table_name}: {e}")
        current_date += timedelta(days=1)
        continue

    # Step 2: Preprocessing
    df["time_of_track"] = pd.to_datetime(df["time_of_track"])
    df["altitude_ft"] = df["measured_fl"] * 100
    df.sort_values(["flight_key", "time_of_track"], inplace=True)
    df["alt_diff"] = df.groupby("flight_key")["altitude_ft"].diff()
    df["time_diff"] = df.groupby("flight_key")["time_of_track"].diff().dt.total_seconds()
    df["vertical_rate"] = df["alt_diff"] / df["time_diff"]

    # Step 3: Detect go-arounds per flight
    for flight_key, group in df.groupby("flight_key"):
        if detect_go_around(group):
            go_around_events.append({
                "flight_key": flight_key,
                "date": current_date.date(),
                "samples": len(group),
                "first_time": group["time_of_track"].min()
            })

    current_date += timedelta(days=1)

# === Final output with dynamic filename ===
start_str = START_DATE.strftime('%Y%m%d')
end_str = END_DATE.strftime('%Y%m%d')
output_filename = f"go_around_flights_{start_str}_{end_str}.csv"

df_results = pd.DataFrame(go_around_events)
df_results.to_csv(output_filename, index=False)

print(f"Saved results to {output_filename}")
