import pandas as pd

root_path = "/Users/pongabha/Library/CloudStorage/Dropbox/Workspace/airspace analysis/AirTOP Workspace/Projects"
scenario = "/BKK_FIR_AIRAC-2024-13_BKK_RWY_19-20-21_100_percent"


# --- File Paths ---
csv_path = f"{root_path}{scenario}/flightplan/FlightPlan.csv"

root_path_2 = "/Users/pongabha/Library/CloudStorage/Dropbox/Workspace/airspace analysis/AirTOP Workspace/"
excel_path = f"{root_path_2}/References/transfer of control VTBS_VTBD_PAv1.xlsx"

# --- Load Data ---
df = pd.read_csv(csv_path, delimiter=";", skiprows=10, engine="python")
sid_df = pd.read_excel(excel_path, sheet_name="SID")
star_df = pd.read_excel(excel_path, sheet_name="STAR")

# --- Helper Functions ---
def extract_first_waypoint(waypoint_str):
    if isinstance(waypoint_str, str):
        waypoints = [w.strip() for w in waypoint_str.split(',') if w.strip()]
        return waypoints[0] if waypoints else None
    return None

def extract_last_waypoint(waypoint_str):
    if isinstance(waypoint_str, str):
        waypoints = [w.strip() for w in waypoint_str.split(',') if w.strip()]
        return waypoints[-1] if waypoints else None
    return None

# --- Departures from VTBD/VTBS ---
departures = df[df.iloc[:, 11].isin(['VTBD', 'VTBS'])].copy()
departures['FirstWaypoint'] = departures.iloc[:, 27].apply(extract_first_waypoint)
departures_df = departures[[df.columns[1], df.columns[11], 'FirstWaypoint']]
departures_df.columns = ['Callsign', 'Origin', 'Waypoint']
sid_waypoints = sid_df['SID'].dropna().unique()
departures_df = departures_df[departures_df['Waypoint'].isin(sid_waypoints)]
departures_df = departures_df.merge(
    sid_df,
    left_on=['Waypoint', 'Origin'],
    right_on=['SID', 'Airport'],
    how='left'
)


# --- Arrivals to VTBD/VTBS ---
arrivals = df[df.iloc[:, 12].isin(['VTBD', 'VTBS'])].copy()
arrivals['LastWaypoint'] = arrivals.iloc[:, 27].apply(extract_last_waypoint)
arrivals_df = arrivals[[df.columns[1], df.columns[12], 'LastWaypoint']]
arrivals_df.columns = ['Callsign', 'Destination', 'Waypoint']
star_waypoints = star_df['STAR'].dropna().unique()
arrivals_df = arrivals_df[arrivals_df['Waypoint'].isin(star_waypoints)]
# Corrected STAR match (includes both Waypoint and Destination-Airport match)
arrivals_df = arrivals_df.merge(
    star_df,
    left_on=['Waypoint', 'Destination'],
    right_on=['STAR', 'Airport'],
    how='left'
)


# --- Save Results ---
departures_df.to_excel(f"{root_path}{scenario}/Mapped_SID_Sectors.xlsx", index=False)
arrivals_df.to_excel(f"{root_path}{scenario}/Mapped_STAR_Sectors.xlsx", index=False)

print("✅ SID and STAR sector mapping completed.")
print("📁 Output files: Mapped_SID_Sectors.xlsx, Mapped_STAR_Sectors.xlsx")
