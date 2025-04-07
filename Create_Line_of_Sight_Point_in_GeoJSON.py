import numpy as np
import rasterio
import geopandas as gpd
import matplotlib.pyplot as plt
from shapely.geometry import Point
from geopy.distance import geodesic
from rasterio.plot import show
from rasterio.windows import from_bounds
import os


def load_dem(dem_path):
    """Loads the Digital Elevation Model (DEM) from a raster file."""
    return rasterio.open(dem_path)

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
    """Gets elevation from DEM at a specific latitude/longitude."""
    try:
        row, col = dem_data.index(lon, lat)
        return dem_data.read(1)[row, col]
    except (IndexError, ValueError):
        return np.nan  # Return NaN if out of DEM bounds or invalid coords


def check_los(tx_location, rx_location, dem_data, tx_height=50):
    """
    Checks line-of-sight (LOS) between a transmitter and receiver,
    considering terrain elevation and Earth curvature.
    """
    num_steps = 100
    lat_steps = np.linspace(tx_location[0], rx_location[0], num_steps)
    lon_steps = np.linspace(tx_location[1], rx_location[1], num_steps)

    tx_elev = get_elevation(dem_data, tx_location[1], tx_location[0]) + tx_height
    rx_elev = get_elevation(dem_data, rx_location[1], rx_location[0])

    if np.isnan(tx_elev) or np.isnan(rx_elev):
        return False

    earth_radius = 6371000  # Earth's radius in meters
    total_distance = geodesic(tx_location, rx_location).meters
    step_distance = total_distance / num_steps

    for i in range(1, num_steps):  # Skip first point (TX location)
        lat, lon = lat_steps[i], lon_steps[i]
        terrain_elev = get_elevation(dem_data, lon, lat)
        if np.isnan(terrain_elev):
            continue

        # Height of LOS at this step, considering Earth curvature
        los_height = tx_elev - (i * step_distance) ** 2 / (2 * earth_radius)

        if terrain_elev > los_height:
            return False  # Blocked by terrain

    return True  # Clear LOS


def generate_coverage_map(tx_location, max_range_km, dem_data, tx_height=50, step_size_km=2.5, angle_step_deg=2):
    """
    Generates a VHF radio coverage map using LOS method.
    """
    coverage_points = []

    for bearing in range(0, 360, angle_step_deg):
        for d in np.arange(step_size_km, max_range_km + step_size_km, step_size_km):
            end_point = geodesic(kilometers=d).destination(tx_location, bearing)
            rx_location = (end_point.latitude, end_point.longitude)

            if check_los(tx_location, rx_location, dem_data, tx_height=tx_height):
                coverage_points.append(Point(rx_location[1], rx_location[0]))  # (lon, lat)

    gdf = gpd.GeoDataFrame(geometry=coverage_points, crs="EPSG:4326")
    gdf["id"] = range(len(gdf))  # Add simple ID field
    return gdf


def plot_coverage_map(dem_data, coverage_gdf, tx_location=None):
    """
    Plots the coverage map with optional DEM background and TX marker.
    """
    fig, ax = plt.subplots(figsize=(10, 10))
    show(dem_data, ax=ax, alpha=0.3, cmap="terrain")
    coverage_gdf.plot(ax=ax, markersize=3, color="blue", alpha=0.6, label="Coverage Points")

    if tx_location:
        ax.plot(tx_location[1], tx_location[0], marker='*', color='red', markersize=12, label='Transmitter')

    plt.title("VHF Radio Coverage Area")
    plt.xlabel("Longitude")
    plt.ylabel("Latitude")
    plt.grid(True)
    plt.legend()
    plt.tight_layout()
    plt.show()


def export_to_geojson(gdf, output_path):
    """
    Exports the GeoDataFrame to a GeoJSON file.
    """
    gdf.to_file(output_path, driver="GeoJSON")
    print(f"GeoJSON file exported to: {output_path}")


# --------------------------
# MAIN EXECUTION
# --------------------------

# Parameters
dem_path = "/Users/pongabha/Library/CloudStorage/Dropbox/Workspace/BearCat Aviation/Bhutan/Bhutan_raster_dem.tif"
tx_height = 50                           # Transmitter height above terrain (m)
max_range_km = 200 * 1.852                 # Max range in km (200 NM)
step_size_km = 2.5                         # Radial sampling distance
angle_step_deg = 2                         # Angular resolution
output_dir = "/Users/pongabha/Library/CloudStorage/Dropbox/Workspace/BearCat Aviation/Bhutan/"
output_geojson_path = "/Users/pongabha/Library/CloudStorage/Dropbox/Workspace/BearCat Aviation/Bhutan/VQGP_vhf_coverage_fixed.geojson"

# Define multiple transmitter locations
tx_locations = {
    "VQGP_Yonphula": (26.88457222, 90.46416111),
    "VQPR_Paro": (27.403226, 89.424553),
    "VQBT_Bumthang": (27.5593, 90.7460),
    "VQTY_Tashigang": (27.3315, 91.5533)
}

# Main loop for each transmitter
for tx_name, tx_location in tx_locations.items():
    print(f"Processing {tx_name} at {tx_location}...")

    # Create subdirectory
    subdir = os.path.join(output_dir, tx_name)
    os.makedirs(subdir, exist_ok=True)

    # Load DEM subset centered on this transmitter
    dem_data = load_dem_subset(dem_path, tx_location[0], tx_location[1], buffer_km=10)

    # Loop over TX heights
    for tx_height in range(0, 1001, 100):
        coverage_gdf = generate_coverage_map(tx_location, max_range_km, dem_data,
                                             tx_height=tx_height,
                                             step_size_km=step_size_km,
                                             angle_step_deg=angle_step_deg)

        filename = f"{tx_name}_vhf_coverage_{tx_height:03d}m.geojson"
        output_path = os.path.join(subdir, filename)
        export_to_geojson(coverage_gdf, output_path)
