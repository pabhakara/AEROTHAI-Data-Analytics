import geopandas as gpd
from shapely.geometry import Point, Polygon
import matplotlib.pyplot as plt
import numpy as np

def create_sector(center, radius_m, start_angle, end_angle, num_points=100):
    """
    Create a sector as a polygon centered at a given point in UTM coordinates.
    """
    angles = np.linspace(np.radians(start_angle), np.radians(end_angle), num_points)
    points = [
        (
            center.x + radius_m * np.cos(angle),
            center.y + radius_m * np.sin(angle)
        )
        for angle in angles
    ]
    return Polygon([center] + points + [center])

# Sample DME data
# Replace this with your actual dataset (e.g., from a CSV file)
dme_data = [
    {"id": "DME1", "latitude": 13.7563, "longitude": 100.5018, "radius_km": 50},
    {"id": "DME2", "latitude": 14.6760, "longitude": 100.6426, "radius_km": 60},
    {"id": "DME3", "latitude": 15.8700, "longitude": 100.9925, "radius_km": 70},
]

# Convert DME data into a GeoDataFrame with initial WGS84 (EPSG:4326) CRS
dme_geometries = [
    Point(dme["longitude"], dme["latitude"]) for dme in dme_data
]
dme_gdf = gpd.GeoDataFrame(dme_data, geometry=dme_geometries, crs="EPSG:4326")

# Reproject to UTM
# Automatically determine UTM zone based on centroid longitude
utm_crs = dme_gdf.estimate_utm_crs()
dme_gdf = dme_gdf.to_crs(utm_crs)

# Create sector coverage for each DME in UTM
start_angle = 30  # Starting azimuth in degrees
end_angle = 150   # Ending azimuth in degrees

dme_gdf["coverage_sector"] = [
    create_sector(
        dme.geometry,
        dme["radius_km"] * 1000,  # Convert radius to meters
        start_angle,
        end_angle
    )
    for _, dme in dme_gdf.iterrows()
]

# Generate combined sector coverage for DME/DME
combined_coverage_sector = gpd.GeoSeries(dme_gdf["coverage_sector"]).unary_union

# Convert combined coverage to GeoDataFrame for plotting
combined_coverage_sector_gdf = gpd.GeoDataFrame(
    {"id": ["combined_sector"], "geometry": [combined_coverage_sector]}, crs=utm_crs
)

# Plot the map
fig, ax = plt.subplots(1, 1, figsize=(10, 10))

# Plot individual DME coverage sectors and combined sector coverage
dme_gdf.set_geometry("coverage_sector").plot(ax=ax, color="blue", alpha=0.5, label="DME Sector Coverage")
combined_coverage_sector_gdf.plot(ax=ax, color="green", alpha=0.3, label="Combined Sector Coverage")

# Add legend and labels
plt.legend()
plt.title(f"DME/DME Coverage Map (30° to 150° Azimuth) - UTM Zone {utm_crs.to_string()}")
plt.xlabel("Easting (m)")
plt.ylabel("Northing (m)")
plt.show()
