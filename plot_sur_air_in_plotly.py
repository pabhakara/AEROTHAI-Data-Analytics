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
input_flight_key = '%THA%'  # Use wildcard pattern for LIKE query

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
selected_df['flight_key_short'] = selected_df['flight_key'].str.slice(0, 15)


# --- Generate interactive map using Plotly ---
import plotly.express as px


fig = px.line_mapbox(
    selected_df.sort_values(['flight_key', 'app_time']),
    lat='latitude',
    lon='longitude',
    color='flight_key_short',
    hover_name='flight_key',
    height=700,
    line_group='flight_key_short',
    title='Flight Paths'
)

lat_min = selected_df['latitude'].min()
lat_max = selected_df['latitude'].max()
lon_min = selected_df['longitude'].min()
lon_max = selected_df['longitude'].max()

lat_pad = (lat_max - lat_min) * 0.05
lon_pad = (lon_max - lon_min) * 0.05

fig.update_layout(
    mapbox_style='carto-darkmatter',
    mapbox=dict(
        center={"lat": (lat_min + lat_max) / 2, "lon": (lon_min + lon_max) / 2},
        zoom=4  # default zoom; user can zoom further in HTML
    )
)
fig.update_traces(line=dict(width=2), opacity=0.4)

mapbox_html = "matched_flights_plotly_map.html"
fig.write_html(mapbox_html)
os.system(f'start {mapbox_html}' if os.name == 'nt' else f'open {mapbox_html}')
conn.close()
