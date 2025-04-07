import geopandas as gpd
import plotly.graph_objects as go
from sqlalchemy import create_engine
import urllib.parse
from shapely.ops import unary_union

# --- PostgreSQL connection info ---
pg_user = "postgres"
pg_password = "password"
pg_host = "127.0.0.1"
pg_port = "5432"
pg_database = "temp"
search_path = "dbo,aireon,public"

pg_password_encoded = urllib.parse.quote_plus(pg_password)
db_url = (
    f"postgresql://{pg_user}:{pg_password_encoded}@{pg_host}:{pg_port}/{pg_database}"
    f"?options=-csearch_path={search_path}"
)
engine = create_engine(db_url)

# --- Query with ICAO codes ---
sql = """
SELECT
    departure,
    arrival,
    geom
FROM "LogBook-20241211_tracks"
WHERE geom IS NOT NULL
AND distance_km > 5000
"""
gdf = gpd.read_postgis(sql, engine, geom_col='geom')

# --- Cluster by departure → arrival ICAO codes ---
gdf['route'] = gdf['departure'] + ' → ' + gdf['arrival']
grouped = gdf.groupby('route')['geom'].apply(lambda x: unary_union(x)).reset_index()
grouped['count'] = gdf.groupby('route')['geom'].count().values

# --- Create Plotly traces for each route ---
traces = []

# Create Plotly traces for each grouped route
for _, row in grouped.iterrows():
    route = row['route']
    geom = row['geom']
    count = row['count']

    # Handle both LineString and MultiLineString
    if geom.geom_type == 'LineString':
        lines = [geom]
    elif geom.geom_type == 'MultiLineString':
        lines = list(geom.geoms)
    else:
        continue

    hover_text = f"{route}<br>Flights: {count}"

    for i, line in enumerate(lines):
        lons, lats = zip(*list(line.coords))
        traces.append(go.Scattergeo(
            lon=lons,
            lat=lats,
            mode='lines',
            line=dict(width=1, color='yellow'),
            opacity=0.1,
            name=route if i == 0 else None,
            showlegend=(i == 0),
            text=hover_text,
            hoverinfo='text'
        ))


# --- Build interactive globe ---
fig = go.Figure(data=traces)

fig.update_geos(
    projection_type="orthographic",
    showland=True,
    showocean=True,
    showcoastlines=True,
    landcolor="rgb(20, 20, 20)",
    oceancolor="rgb(10, 10, 40)",
    coastlinecolor="gray",
    bgcolor="black"
)

fig.update_layout(
    title="Clustered Great Circle Routes by ICAO Departure–Arrival",
    height=800,
    paper_bgcolor="black",
    plot_bgcolor="black",
    font_color="white",
    legend=dict(bgcolor="rgba(0,0,0,0)", bordercolor="white")
)

fig.show()
