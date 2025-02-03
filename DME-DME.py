import geopandas as gpd
import psycopg2
from sqlalchemy import create_engine
import matplotlib.pyplot as plt
from shapely.geometry import Point
import numpy as np
import geoalchemy2

# Function to calculate azimuth (bearing) between two points
def calculate_azimuth(point1, point2):
    """
    Calculate azimuth (in degrees) from point1 to point2.
    """
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
TABLE_NAME = "cns_coverage.dme_coverage"  # Replace with the table name containing DME coverages


# conn_postgres_target = psycopg2.connect(user = "postgres",
#                                   password = "password",
#                                   host = "127.0.0.1",
#                                   port = "5432",
#                                   database = "temp",
#                                   options="-c search_path=dbo,public")

# Connect to PostgreSQL database
engine = create_engine(f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}")

flevel = 10000
# Query DME coverage polygons from PostgreSQL
query = f"""
SELECT site_id, vor_latitude, vor_longitude, geom
FROM {TABLE_NAME}
WHERE flevel = {flevel} AND NOT site_id = 'TRT'
"""
# Load the data into a GeoDataFrame

print(query)
dme_gdf = gpd.read_postgis(query, engine, geom_col="geom")
dme_gdf = dme_gdf.explode()

print(dme_gdf)

# Check the CRS and reproject to UTM for accurate calculations
# if dme_gdf.crs.to_string() != "EPSG:32648":  # Replace 'EPSG:32648' with your UTM zone CRS
#     dme_gdf = dme_gdf.to_crs("EPSG:32648")

dme_gdf.to_crs("EPSG:3857")

# Calculate azimuths and filter pairs
valid_pairs = []
for i, dme1 in dme_gdf.iterrows():
    for j, dme2 in dme_gdf.iterrows():
        if i < j:  # Avoid duplicates and self-pairing
            azimuth = calculate_azimuth(dme1.geom.centroid, dme2.geom.centroid)
            if 30 <= azimuth <= 150:
                valid_pairs.append((dme1, dme2))

# Calculate overlaps for valid pairs
overlap_polygons = []
for dme1, dme2 in valid_pairs:
    overlap = dme1.geom.intersection(dme2.geom)
    if not overlap.is_empty:
        overlap_polygons.append(overlap)

# Combine all valid overlaps into a single coverage area
combined_coverage = gpd.GeoSeries(overlap_polygons).union_all()

# Convert combined coverage to GeoDataFrame
combined_coverage_gdf = gpd.GeoDataFrame(
    {"geometry": [combined_coverage]}, crs=dme_gdf.crs
)

# #Reproject back to WGS-84 (EPSG:4326)
# combined_coverage_gdf = combined_coverage_gdf.to_crs("EPSG:4326")
# dme_gdf = dme_gdf.to_crs("EPSG:4326")

# Export the results to PostgreSQL
# Save individual DME coverage with buffer
dme_gdf.to_postgis("dme_buffers", engine, if_exists="replace")

# Save combined coverage
combined_coverage_gdf.to_postgis("dme_combined_coverage", engine, if_exists="replace")

# Save overlap polygons
overlap_gdf = gpd.GeoDataFrame({"geometry": overlap_polygons}, crs="EPSG:4326")
overlap_gdf.to_postgis("dme_overlaps", engine, if_exists="replace")

# Plot the map
fig, ax = plt.subplots(1, 1, figsize=(10, 10))

# Plot individual DME coverages, valid overlaps, and combined coverage
dme_gdf.plot(ax=ax, color="blue", alpha=0.3, label="Individual DME Coverages")

gpd.GeoDataFrame({"geometry": overlap_polygons}).plot(
    ax=ax, color="green", alpha=0.5, label="Valid Overlaps"
)

combined_coverage_gdf.plot(ax=ax, color="yellow", alpha=0.5, label="Combined Coverage")

# Add legend and labels
plt.legend()
plt.title(f"DME/DME Coverage Map at {flevel} ft")
plt.xlabel("Easting (m)")
plt.ylabel("Northing (m)")
plt.show()