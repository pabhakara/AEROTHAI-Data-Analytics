import numpy as np
import rasterio
import geopandas as gpd
import matplotlib.pyplot as plt
from shapely.geometry import Point
from geopy.distance import geodesic
from rasterio.plot import show
from rasterio.windows import from_bounds
import os
import alphashape



def load_dem_subset(dem_path, center_lat, center_lon, buffer_km=50):
    """
    Loads a windowed subset of the DEM around the transmitter location.
    `buffer_km` defines how large the square area (in km) to extract.
    """
    with rasterio.open(dem_path) as src:
        # Approximate degree buffer using 1 deg ≈ 111 km (lat)
        buffer_deg = buffer_km / 111.0
        min_lon = center_lon - buffer_deg
        max_lon = center_lon + buffer_deg
        min_lat = center_lat - buffer_deg
        max_lat = center_lat + buffer_deg

        # Create window from bounds
        window = from_bounds(min_lon, min_lat, max_lon, max_lat, src.transform)
        window = window.round_offsets().round_lengths()

        # Read the windowed data
        dem_subset = src.read(1, window=window)
        transform = src.window_transform(window)
        crs = src.crs

    # Return a MemoryFile-like object
    from rasterio.io import MemoryFile
    memfile = MemoryFile()
    with memfile.open(
            driver="GTiff",
            height=dem_subset.shape[0],
            width=dem_subset.shape[1],
            count=1,
            dtype=dem_subset.dtype,
            crs=crs,
            transform=transform,
    ) as dst:
        dst.write(dem_subset, 1)
    return memfile.open()



def get_elevation(dem_data, lon, lat):
    try:
        row, col = dem_data.index(lon, lat)
        return dem_data.read(1)[row, col]
    except (IndexError, ValueError):
        return np.nan

def check_los(tx_location, rx_location, dem_data, tx_height=50):
    num_steps = 100
    lat_steps = np.linspace(tx_location[0], rx_location[0], num_steps)
    lon_steps = np.linspace(tx_location[1], rx_location[1], num_steps)

    tx_elev = get_elevation(dem_data, tx_location[1], tx_location[0]) + tx_height
    rx_elev = get_elevation(dem_data, rx_location[1], rx_location[0])

    if np.isnan(tx_elev) or np.isnan(rx_elev):
        return False

    earth_radius = 6371000
    total_distance = geodesic(tx_location, rx_location).meters
    step_distance = total_distance / num_steps

    for i in range(1, num_steps):
        lat, lon = lat_steps[i], lon_steps[i]
        terrain_elev = get_elevation(dem_data, lon, lat)
        if np.isnan(terrain_elev):
            continue

        los_height = tx_elev - (i * step_distance) ** 2 / (2 * earth_radius)
        if terrain_elev > los_height:
            return False

    return True

import alphashape

from shapely.geometry import Polygon

def generate_coverage_boundary_polygon(tx_location, max_range_km, dem_data, tx_height=50,
                                       step_size_km=2.5, angle_step_deg=2):
    """
    Generates a coverage boundary polygon by connecting the outermost visible points
    along each radial direction.
    """
    boundary_points = []

    for bearing in range(0, 360, angle_step_deg):
        last_visible_point = None

        for d in np.arange(step_size_km, max_range_km + step_size_km, step_size_km):
            end_point = geodesic(kilometers=d).destination(tx_location, bearing)
            rx_location = (end_point.latitude, end_point.longitude)

            if check_los(tx_location, rx_location, dem_data, tx_height=tx_height):
                last_visible_point = (rx_location[1], rx_location[0])  # (lon, lat)
            else:
                break  # Stop if LOS is blocked

        if last_visible_point:
            boundary_points.append(last_visible_point)

    if len(boundary_points) >= 3:
        # Close the polygon by repeating the first point
        boundary_points.append(boundary_points[0])
        polygon = Polygon(boundary_points)
    else:
        polygon = None

    if polygon and polygon.is_valid:
        return gpd.GeoDataFrame({"tx_height_m": [tx_height]}, geometry=[polygon], crs="EPSG:4326")
    else:
        return None


def export_polygon_to_geojson(gdf, output_path):
    gdf.to_file(output_path, driver="GeoJSON")
    print(f"Exported: {output_path}")

# --------------------------
# MAIN EXECUTION
# --------------------------

dem_path = "/Users/pongabha/Library/CloudStorage/Dropbox/Workspace/BearCat Aviation/Bhutan/Bhutan_raster_dem.tif"
tx_location = (27.40311944, 89.42480556)
max_range_km = 200 * 1.852
step_size_km = 2.5
angle_step_deg = 2
output_dir = "/Users/pongabha/Library/CloudStorage/Dropbox/Workspace/BearCat Aviation/Bhutan/tx_height_polygons"

os.makedirs(output_dir, exist_ok=True)
# Load DEM
#dem_data = load_dem(dem_path)
dem_data = load_dem_subset(dem_path, tx_location[0], tx_location[1], buffer_km=10)

for tx_height in range(0, 1001, 100):
    gdf = generate_coverage_boundary_polygon(tx_location, max_range_km, dem_data,
                                     tx_height=tx_height,
                                     step_size_km=step_size_km,
                                     angle_step_deg=angle_step_deg)
    if gdf is not None:
        filename = f"vhf_coverage_{tx_height:03d}m.geojson"
        output_path = os.path.join(output_dir, filename)
        export_polygon_to_geojson(gdf, output_path)
    else:
        print(f"Skipping {tx_height} m — not enough points to form a polygon.")
