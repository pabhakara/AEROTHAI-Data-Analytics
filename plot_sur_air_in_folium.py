import pandas as pd
import folium
from folium import plugins
import os
import branca.colormap as cm
import matplotlib.colors as mcolors
import plotly.express as px
import matplotlib.pyplot as plt
from sqlalchemy import create_engine
from datetime import datetime, timedelta

# --- Connect to PostgreSQL using psycopg2 ---
import psycopg2

conn = psycopg2.connect(
    dbname="temp",
    user="postgres",
    password="password",
    host="127.0.0.1",
    port="5432",
    options="-c search_path=dbo,sur_air"
)

# --- User input flight key ---
input_flight_key = '%BKP%'  # Use wildcard pattern for LIKE query

# --- Search for flight_key in a specific table ---
yyyy = '2025'
mm = '03'
dd = '27'
table = f"cat062_{yyyy}{mm}{dd}"

query = f"""
    SELECT * FROM sur_air.{table}
    WHERE flight_key LIKE '{input_flight_key}' AND latitude IS NOT NULL AND longitude IS NOT NULL
    ORDER BY app_time;
"""

selected_df = pd.read_sql(query, conn)
if selected_df.empty:
    print("Flight not found.")
    exit()

selected_df['app_time'] = pd.to_datetime(selected_df['app_time'])


# --- Generate map for all matched flights ---
map_center = [selected_df['latitude'].mean(), selected_df['longitude'].mean()]
flight_map = folium.Map(location=map_center, zoom_start=6, tiles='CartoDB dark_matter')


for flight_key, group in selected_df.groupby('flight_key'):
    group = group.sort_values('app_time').reset_index(drop=True)
    for i in range(len(group) - 1):
        segment = [
            (group.loc[i, 'latitude'], group.loc[i, 'longitude']),
            (group.loc[i + 1, 'latitude'], group.loc[i + 1, 'longitude'])
        ]
        folium.PolyLine(segment, color='yellow', weight=2, opacity=0.3, tooltip=flight_key).add_to(flight_map)

map_html = "matched_flights_map.html"
flight_map.save(map_html)

# --- Open plots in browser ---
os.system(f'start {map_html}' if os.name == 'nt' else f'open {map_html}')

# --- Close the database connection ---
conn.close()
