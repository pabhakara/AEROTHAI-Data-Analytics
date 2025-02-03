import numpy as np
from math import radians, cos, sin, sqrt, atan2


def calculate_distance(lat1, lon1, lat2, lon2):
    R = 6371e3  # Earth's radius in meters

    phi1, phi2 = radians(lat1), radians(lat2)
    delta_phi = radians(lat2 - lat1)
    delta_lambda = radians(lon2 - lon1)

    a = sin(delta_phi / 2) ** 2 + cos(phi1) * cos(phi2) * sin(delta_lambda / 2) ** 2
    c = 2 * atan2(sqrt(a), sqrt(1 - a))

    return R * c


def raim_prediction(user_position, satellite_positions):
    """
    Predicts RAIM based on user position and satellite positions.

    Parameters:
        user_position (tuple): Latitude and Longitude of the user in degrees (lat, lon).
        satellite_positions (list): List of satellite positions as tuples (lat, lon, altitude).

    Returns:
        dict: Contains HPL, VPL, and satellite geometry matrix.
    """
    user_lat, user_lon = user_position

    # Compute satellite-user range vectors
    range_vectors = []
    for sat_pos in satellite_positions:
        sat_lat, sat_lon, sat_alt = sat_pos
        distance = calculate_distance(user_lat, user_lon, sat_lat, sat_lon)
        range_vectors.append([sat_lat - user_lat, sat_lon - user_lon, sat_alt])

    range_vectors = np.array(range_vectors)

    # Satellite geometry matrix (simplified for demonstration)
    G = np.linalg.pinv(range_vectors.T)  # Pseudoinverse of geometry matrix

    # Protection levels based on simplified covariance approximation
    HPL = np.sqrt(np.sum(np.diag(G @ G.T))) * 5.33  # HPL scale factor
    VPL = np.sqrt(np.sum(np.diag(G @ G.T))) * 6.00  # VPL scale factor

    return {
        "HPL": HPL,
        "VPL": VPL,
        "Geometry Matrix": G
    }


# Example Usage
if __name__ == "__main__":
    user_position = (13.7563, 100.5018)  # Bangkok coordinates
    satellite_positions = [
        (20.0, 100.0, 20200e3),  # Simulated satellite positions
        (25.0, 102.0, 20200e3),
        (30.0, 104.0, 20200e3),
        (35.0, 106.0, 20200e3),
        (40.0, 108.0, 20200e3)
    ]

    prediction = raim_prediction(user_position, satellite_positions)

    print("Horizontal Protection Level (HPL):", prediction["HPL"])
    print("Vertical Protection Level (VPL):", prediction["VPL"])
    print("Satellite Geometry Matrix:\n", prediction["Geometry Matrix"])
