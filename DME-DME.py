import geopandas as gpd
import psycopg2
from sqlalchemy import create_engine
import matplotlib.pyplot as plt
from shapely.geometry import Point
import numpy as np
import geoalchemy2
import matplotlib.patches as mpatches

# Function to calculate azimuth (bearing) between two points
def calculate_azimuth(point1, point2):
    lon1, lat1 = np.radians(point1.x), np.radians(point1.y)
    lon2, lat2 = np.radians(point2.x), np.radians(point2.y)

    dlon = lon2 - lon1
    x = np.sin(dlon) * np.cos(lat2)
    y = np.cos(lat1) * np.sin(lat2) - (np.sin(lat1) * np.cos(lat2) * np.cos(dlon))
    azimuth = (np.degrees(np.arctan2(x, y)) + 360) % 360
    return azimuth

# Database connection details
DB_HOST = "localhost"
DB_PORT = "5432"
DB_NAME = "temp"
DB_USER = "postgres"
DB_PASSWORD = "password"
TABLE_NAME = "cns_coverage.dme_coverage"  # Replace with your table

# Connect to PostgreSQL
engine = create_engine(f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}")

flevel = 11000

# Query DME coverage
query = f"""
SELECT site_id, vor_latitude, vor_longitude, geom
FROM {TABLE_NAME}
WHERE flevel = {flevel}
"""
print(query)

# Load into GeoDataFrame
dme_gdf = gpd.read_postgis(query, engine, geom_col="geom")
dme_gdf = dme_gdf.explode(index_parts=False)
dme_gdf = dme_gdf.to_crs("EPSG:3857")

# Compute azimuth-filtered pairs
valid_pairs = []
for i, dme1 in dme_gdf.iterrows():
    for j, dme2 in dme_gdf.iterrows():
        if i < j:
            azimuth = calculate_azimuth(dme1.geom.centroid, dme2.geom.centroid)
            if 30 <= azimuth <= 150:
                valid_pairs.append((dme1, dme2))

# Compute overlaps
overlap_polygons = []
for dme1, dme2 in valid_pairs:
    overlap = dme1.geom.intersection(dme2.geom)
    if not overlap.is_empty:
        overlap_polygons.append(overlap)

# Combine all overlaps
combined_coverage = gpd.GeoSeries(overlap_polygons).union_all()
combined_coverage_gdf = gpd.GeoDataFrame({"geometry": [combined_coverage]}, crs=dme_gdf.crs)

# Export to PostGIS
dme_gdf.to_postgis("dme_buffers", engine, if_exists="replace", index=False)
combined_coverage_gdf.to_postgis("dme_combined_coverage", engine, if_exists="replace", index=False)
overlap_gdf = gpd.GeoDataFrame({"geometry": overlap_polygons}, crs=dme_gdf.crs)
overlap_gdf.to_postgis("dme_overlaps", engine, if_exists="replace", index=False)

# Plotting
fig, ax = plt.subplots(1, 1, figsize=(10, 10))

# Plot each layer
dme_gdf.plot(ax=ax, color="blue", alpha=0.3)
gpd.GeoDataFrame({"geometry": overlap_polygons}, crs=dme_gdf.crs).plot(ax=ax, color="green", alpha=0.5)
combined_coverage_gdf.plot(ax=ax, color="yellow", alpha=0.5)

# Create legend manually
patch1 = mpatches.Patch(color="blue", alpha=0.3, label="Individual DME Coverages")
patch2 = mpatches.Patch(color="green", alpha=0.5, label="Valid Overlaps")
patch3 = mpatches.Patch(color="yellow", alpha=0.5, label="Combined Coverage")
ax.legend(handles=[patch1, patch2, patch3], loc="upper right")

# Add axis/title
ax.set_title(f"DME/DME Coverage Map at {flevel} ft")
ax.set_xlabel("Easting (m)")
ax.set_ylabel("Northing (m)")
ax.set_aspect("equal")
ax.grid(True)

# Save figure
fig.savefig("dme_coverage_map.png", dpi=300)
# plt.show()  # Uncomment to display interactively
