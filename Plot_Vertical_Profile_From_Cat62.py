import psycopg2
import psycopg2.extras
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.ticker import (AutoMinorLocator, MultipleLocator)
from matplotlib.dates import DateFormatter
import os
import glob
import plotly.express as px
from plotly.subplots import make_subplots
import plotly.graph_objects as go
import datetime as dt
from datetime import date, timedelta


# PostgreSQL connection
# conn_postgres_source = psycopg2.connect(
#     user="pongabhaab",
#     password="pongabhaab2",
#     host="172.16.129.241",
#     port="5432",
#     database="aerothai_dwh",
#     options="-c search_path=dbo,sur_air"
# )

conn_postgres_source = psycopg2.connect(
            user="postgres",
            password="password",
            host="127.0.0.1",
            port="5432",
            database="temp",
            options="-c search_path=dbo,public")

# Output folder
output_filepath = '/Users/pongabha/Dropbox/Workspace/AEROTHAI Data Analytics/Flight_Proflie_Plots/'
files = glob.glob(f"{output_filepath}*")
for f in files:
    os.remove(f)

date_list = pd.date_range(start='2025-03-01', end='2025-03-31')
date_range_start = date_list[0].strftime('%Y%m%d')
date_range_end = date_list[-1].strftime('%Y%m%d')
output_name_suffix = f"{date_range_start}_{date_range_end}"


# Initialize global figure
combined_fig = make_subplots(specs=[[{"secondary_y": True}]])

# Optional: store metadata for final summary CSV
combined_summary_records = []

for current_date in date_list:
    # Define current date components
    year_str = str(current_date.year)
    month_str = f"{current_date.month:02d}"
    day_str = f"{current_date.day:02d}"

    next_date = current_date + timedelta(days=1)

    # Format next date
    next_year= str(next_date.year)
    next_month = f"{next_date.month:02d}"
    next_day = f"{next_date.day:02d}"

    print(f"\n🛫 Processing {year_str}-{month_str}-{day_str}...")

    with conn_postgres_source:
        cursor_postgres_source = conn_postgres_source.cursor(cursor_factory=psycopg2.extras.DictCursor)

        sql_flight_list = f"""
            SELECT DISTINCT t.flight_key, f.actype
            FROM (
                SELECT * FROM sur_air.cat062_{year_str}{month_str}{day_str}
                UNION ALL
                SELECT * FROM sur_air.cat062_{next_year}{next_month}{next_day}
            ) t
            LEFT JOIN flight_data.flight_{year_str}{month_str} f 
                ON t.flight_id = f.id 
            WHERE 'RYN' = ANY(f.fdmc_bkk_fix)
              AND f.item15_route LIKE '%W42%'
              AND NOT (f.dest LIKE 'VTBU' OR f.dep LIKE 'VTBU')
            ORDER BY t.flight_key ASC;
        """


        try:
            cursor_postgres_source.execute(sql_flight_list)
            records = cursor_postgres_source.fetchall()
        except Exception as e:
            print(f"⚠️ Skipped {year_str}-{month_str}-{day_str} due to error: {e}")
            continue

        if not records:
            print(f"ℹ️ No matching flights on {year_str}-{month_str}-{day_str}")
            continue

        summary_df = pd.DataFrame(records, columns=['flight_key', 'actype'])
        flight_key_list = list(summary_df['flight_key'])

    for flight_key in flight_key_list:
        print(f"✈️  Plotting {flight_key}...")
        cursor_postgres_source = conn_postgres_source.cursor(cursor_factory=psycopg2.extras.DictCursor)

        sql_track = f"""
            SELECT t.flight_key, t.app_time, t.sector, t.dist_from_last_position, t.measured_fl,
                   t.final_state_selected_alt_dap, t.latitude, t.longitude
            FROM sur_air.cat062_{year_str}{month_str}{day_str} t
            LEFT JOIN flight_data.flight_{year_str}{month_str} f 
            ON t.flight_id = f.id 
            WHERE f.flight_key = '{flight_key}'
            AND NOT t.sector IS NULL and t.ground_speed > 20 
            AND t.measured_fl > 70
            ORDER BY t.app_time;
        """

        try:
            cursor_postgres_source.execute(sql_track)
            record = cursor_postgres_source.fetchall()
        except Exception as e:
            print(f"❌ Error loading track for {flight_key}: {e}")
            continue

        if not record:
            continue

        df = pd.DataFrame(record, columns=[
            'flight_key', 'app_time', 'sector', 'dist_from_last_position',
            'measured_fl', 'final_state_selected_alt_dap', 'latitude', 'longitude'
        ])

        accumulated_distance = df['dist_from_last_position'].cumsum()
        accumulated_time = df['app_time'] - df['app_time'].iloc[0]
        accumulated_time = accumulated_time.dt.total_seconds() / 60  # minutes

        df['accumulated_distance'] = accumulated_distance
        df['accumulated_time'] = accumulated_time

        # Add to combined figure
        combined_fig.add_trace(go.Scatter(
            name=f"{flight_key} ({year_str}-{month_str}-{day_str})",
            x=df["accumulated_distance"],
            y=df["measured_fl"] * 100,
            mode='lines',
            line=dict(color='rgba(0,0,0,0.3)', width=1),
            hovertemplate=(
                f"<b>Flight:</b> {flight_key}<br>"
                f"<b>Date:</b> {year_str}-{month_str}-{day_str}<br>"
                "Distance: %{x:.1f} NM<br>"
                "Altitude: %{y:.0f} ft<extra></extra>"
            )
        ), secondary_y=False)

        # Store for summary CSV
        try:
            combined_summary_records.append({
                'date': f"{year_str}-{month_str}-{day_str}",
                'flight_key': flight_key,
                'actype': summary_df[summary_df['flight_key'] == flight_key]['actype'].values[0],
                'track_duration_min': df['accumulated_time'].iloc[-1],
                'track_distance_nm': df['accumulated_distance'].iloc[-1]
            })
        except:
            pass

# Finalize combined figure layout
combined_fig.update_layout(
    title_text="Vertical Profiles of Flight using Route W42 and VOR RYN in March 2025",
    xaxis_title="Accumulated Distance (NM)",
    yaxis_title="Altitude (ft)",
    xaxis=dict(rangeslider=dict(visible=True)),
    height=700
)

html_filename = f"{output_filepath}vertical_profile_combined_{output_name_suffix}.html"
csv_filename = f"{output_filepath}summary_combined_{output_name_suffix}.csv"

# Save summary CSV
summary_df = pd.DataFrame(combined_summary_records)

combined_fig.write_html(html_filename)
print(f"\n✅ Combined plot saved: {html_filename}")

combined_fig.show()

summary_df.to_csv(csv_filename, index=False)
print(f"✅ Combined summary saved: {csv_filename}")


print("🎉 All combined flight profiles processed and saved.")

