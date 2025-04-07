import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime
import re


# Function to read AirTOP CSV files
def pd_read_airtop_csv(airtop_csv_file, dtype_list=None):
    column_names = open(airtop_csv_file).readlines()[1].split(' ')
    airtop_df = pd.read_csv(airtop_csv_file,
                            delimiter=';', header=3,
                            names=column_names,
                            dtype=dtype_list)
    return airtop_df


def pd_sector_entry(temp_df):
    # Filter transitions for SECTOR
    df_sector_entry = temp_df[temp_df['NewCurrentRadarController'].isin(valid_sectors)]
    #df_sector_entry = temp_df[temp_df['NewCurrentRadarController'].str.contains('SECTOR', na=False, regex=True)]
    df_sector_entry = df_sector_entry[df_sector_entry['DurationInPreviousATCSector'] > '00:02:00']
    # Assign Count Columns
    df_sector_entry = df_sector_entry[["Time", "NewCurrentRadarController"]].copy()
    df_sector_entry["Count"] = 1
    df_sector_entry["Workload"] = workload_schema_dict['Sector Entry']['Workload'] / 3600 * 100

    df_sector_entry.rename(columns={"NewCurrentRadarController": "Sector"}, inplace=True)
    df_sector_entry["Time"] = pd.to_datetime(df_sector_entry["Time"])
    df_sector_entry.set_index("Time", inplace=True)
    df_sector_entry.sort_index(inplace=True)

    return df_sector_entry


def pd_sector_exit(temp_df):
    df_sector_exit = temp_df[temp_df['OldCurrentRadarController'].isin(valid_sectors)]
    #df_sector_exit = temp_df[temp_df['OldCurrentRadarController'].str.contains('SECTOR', na=False, regex=True)]
    df_sector_exit = df_sector_exit[df_sector_exit['DurationInPreviousATCSector'] > '00:02:00']

    df_sector_exit = df_sector_exit[["Time", "OldCurrentRadarController"]].copy()
    df_sector_exit["Count"] = 1
    df_sector_exit["Workload"] = workload_schema_dict['Sector Exit']['Workload'] / 3600 * 100

    df_sector_exit.rename(columns={"OldCurrentRadarController": "Sector"}, inplace=True)

    df_sector_exit["Time"] = pd.to_datetime(df_sector_exit["Time"])
    df_sector_exit.set_index("Time", inplace=True)
    df_sector_exit.sort_index(inplace=True)

    return df_sector_exit


def pd_level_change(temp_df):
    df_level_change = temp_df[temp_df['CurrentRadarController'].isin(valid_sectors)]
    #df_level_change = temp_df[temp_df['CurrentRadarController'].str.contains('SECTOR', na=False, regex=True)]

    df_level_change = df_level_change[["Time", "CurrentRadarController"]].copy()
    df_level_change["Count"] = 1
    df_level_change["Workload"] = workload_schema_dict['Level Change']['Workload'] / 3600 * 100

    df_level_change.rename(columns={"CurrentRadarController": "Sector"}, inplace=True)

    df_level_change["Time"] = pd.to_datetime(df_level_change["Time"])
    df_level_change.set_index("Time", inplace=True)
    df_level_change.sort_index(inplace=True)

    return df_level_change


def pd_conflict(temp_df, conflict_type):
    df_conflict = temp_df[temp_df['RadarController'].isin(valid_sectors)]
    #df_conflict = temp_df[temp_df['RadarController'].str.contains('SECTOR', na=False, regex=True)]

    df_conflict = df_conflict[["Time", "RadarController"]].copy()
    df_conflict["Count"] = 1
    df_conflict["Workload"] = workload_schema_dict[conflict_type]['Workload'] / 3600 * 100

    df_conflict.rename(columns={"RadarController": "Sector"}, inplace=True)

    df_conflict["Time"] = pd.to_datetime(df_conflict["Time"])
    df_conflict.set_index("Time", inplace=True)
    df_conflict.sort_index(inplace=True)

    return df_conflict


def pd_convert_time(input_df, year, month):
    input_df['Time'] = input_df['Time'].astype(str)
    input_df['Time'] = f"{year}-{month}-" + input_df['Time']
    input_df['Time'] = pd.to_datetime(input_df['Time'], format="%Y-%m-%d %H:%M:%S")

    return input_df


# Function to process sector workload
def process_sector_workload(workload_df, entry_df):
    # Merge workload and entry count data
    df_combined = pd.merge(workload_df, entry_df, on=["Time", "Sector"], how="inner")

    sectors = df_combined["Sector"].unique()

    for sector in sectors:
        df_sector = df_combined[df_combined["Sector"] == sector]

        fig = go.Figure()

        # Workload trace
        fig.add_trace(go.Scatter(
            x=df_sector["Time"],
            y=df_sector["Workload"],
            mode="lines",
            name="Workload",
            yaxis="y1"
        ))

        # Entry count trace
        fig.add_trace(go.Scatter(
            x=df_sector["Time"],
            y=df_sector["Count"],
            mode="lines",
            name="Entry Count",
            yaxis="y2"
        ))

        # Layout settings
        fig.update_layout(
            title=f"Sector: {sector} - Workload & Entry Count",
            xaxis=dict(title="Time", tickformat="%H:%M"),
            yaxis=dict(title="Workload (%)", side="left", showgrid=True),
            yaxis2=dict(title="Entry Count", overlaying="y", side="right", showgrid=False),
            legend=dict(x=0, y=1),
            template="plotly_white"
        )

        fig.show()


# Function to create dropdown plot
def plot_sector_selectable(workload_df, entry_df):
    df_combined = pd.merge(workload_df, entry_df, on=["Time", "Sector"], how="inner")

    fig = px.line(df_combined, x="Time", y="Workload", color="Sector",
                  title="Sector Workload & Entry Count (Select Sector from Dropdown)",
                  labels={"Workload": "Workload (%)", "Time": "Time"},
                  template="plotly_white")

    fig.show()


# File paths
root_path = "/Users/pongabha/Library/CloudStorage/Dropbox/Workspace/airspace analysis/AirTOP Workspace/Projects"
scenario = "/BKK_FIR_AIRAC-2024-13_BKK_RWY_19-20-21_100_percent"

input_filepath = f"{root_path}{scenario}/report/events/changeevents/"
filenames = ['FLIGHT_ATC_SECTOR_RADAR_CONTROLLER_CHANGED.csv']
output_filepath = f"{root_path}{scenario}/report/"

dict_of_df = {}

workload_schema_dict = {'Sector Entry': {'Workload': 15, 'Monitoring': 5, 'Radio Telephony': 8, 'System Ops': 2},
                        'Sector Exit': {'Workload': 8, 'Radio Telephony': 8, 'System Ops': 2},
                        'Level Change': {'Workload': 8, 'Level Clearance': 8, 'System Ops': 2},
                        'Conflict Detection - crossing both cruising': {'Workload': 20},
                        'Conflict Detection - crossing both in vertical': {'Workload': 30},
                        'Conflict Detection - crossing one in vertical': {'Workload': 25},
                        'Conflict Detection - opposite both cruising': {'Workload': 20},
                        'Conflict Detection - opposite both in vertical': {'Workload': 30},
                        'Conflict Detection - opposite one in vertical': {'Workload': 25},
                        'Conflict Detection - same track both cruising': {'Workload': 10},
                        'Conflict Detection - same track both in vertical': {'Workload': 20},
                        'Conflict Detection - same track one in vertical': {'Workload': 15},
                        'Conflict Resolution - crossing both cruising': {'Workload': 60},
                        'Conflict Resolution - crossing both in vertical': {'Workload': 60},
                        'Conflict Resolution - crossing one in vertical': {'Workload': 60},
                        'Conflict Resolution - opposite both cruising': {'Workload': 60},
                        'Conflict Resolution - opposite both in vertical': {'Workload': 60},
                        'Conflict Resolution - opposite one in vertical': {'Workload': 60},
                        'Conflict Resolution - same track both cruising': {'Workload': 60},
                        'Conflict Resolution - same track both in vertical': {'Workload': 60},
                        'Conflict Resolution - same track one in vertical': {'Workload': 60},
                        }

change_watch_dict = {
    'Sector Entry': 'FLIGHT_ATC_SECTOR_RADAR_CONTROLLER_CHANGED.csv',
    'Sector Exit': 'FLIGHT_ATC_SECTOR_ENTERED.csv',
    'Level Change': 'FLIGHT_GENERAL_ALTITUDE_CHANGE_STARTED.csv',
    'Conflict Detection - crossing both cruising': 'CONFLICT_REAL_CROSSING_BOTH_CRUISING.csv',
    'Conflict Detection - crossing both in vertical': 'CONFLICT_REAL_CROSSING_BOTH_IN_VERTICAL.csv',
    'Conflict Detection - crossing one in vertical': 'CONFLICT_REAL_CROSSING_ONE_IN_VERTICAL.csv',
    'Conflict Detection - opposite both cruising': 'CONFLICT_REAL_OPPOSITE_BOTH_CRUISING.csv',
    'Conflict Detection - opposite both in vertical': 'CONFLICT_REAL_OPPOSITE_BOTH_IN_VERTICAL.csv',
    'Conflict Detection - opposite one in vertical': 'CONFLICT_REAL_OPPOSITE_ONE_IN_VERTICAL.csv',
    'Conflict Detection - same track both cruising': 'CONFLICT_REAL_SAME_TRACK_BOTH_CRUISING.csv',
    'Conflict Detection - same track both in vertical': 'CONFLICT_REAL_SAME_TRACK_BOTH_IN_VERTICAL.csv',
    'Conflict Detection - same track one in vertical': 'CONFLICT_REAL_SAME_TRACK_ONE_IN_VERTICAL.csv',
    'Conflict Resolution - crossing both cruising': 'CONFLICT_REAL_CROSSING_BOTH_CRUISING_SEVERITY_ABOVE_2.csv',
    'Conflict Resolution - crossing both in vertical': 'CONFLICT_REAL_CROSSING_BOTH_IN_VERTICAL_SEVERITY_ABOVE_2.csv',
    'Conflict Resolution - crossing one in vertical': 'CONFLICT_REAL_CROSSING_ONE_IN_VERTICAL_SEVERITY_ABOVE_2.csv',
    'Conflict Resolution - opposite both cruising': 'CONFLICT_REAL_OPPOSITE_BOTH_CRUISING_SEVERITY_ABOVE_2.csv',
    'Conflict Resolution - opposite both in vertical': 'CONFLICT_REAL_OPPOSITE_BOTH_IN_VERTICAL_SEVERITY_ABOVE_2.csv',
    'Conflict Resolution - opposite one in vertical': 'CONFLICT_REAL_OPPOSITE_ONE_IN_VERTICAL_SEVERITY_ABOVE_2.csv',
    'Conflict Resolution - same track both cruising': 'CONFLICT_REAL_SAME_TRACK_BOTH_CRUISING_SEVERITY_ABOVE_2.csv',
    'Conflict Resolution - same track both in vertical': 'CONFLICT_REAL_SAME_TRACK_BOTH_IN_VERTICAL_SEVERITY_ABOVE_2.csv',
    'Conflict Resolution - same track one in vertical': 'CONFLICT_REAL_SAME_TRACK_ONE_IN_VERTICAL_SEVERITY_ABOVE_2.csv', }

#print(workload_schema_dict['Sector Entry']['Workload'])

# Read files
for event in change_watch_dict:
    filename = change_watch_dict[event]
    dict_of_df[filename] = pd_read_airtop_csv(input_filepath + filename, dtype_list=None)

sector_change_df = dict_of_df['FLIGHT_ATC_SECTOR_RADAR_CONTROLLER_CHANGED.csv']
level_change_df = dict_of_df['FLIGHT_GENERAL_ALTITUDE_CHANGE_STARTED.csv']

temp_df = sector_change_df.sort_values(["Callsign", "Time"], ascending=[True, True])

print(temp_df[["Callsign", "Time", "OldCurrentRadarController", "NewCurrentRadarController",
               'OriginInfo', 'DestinationInfo',
               'DurationInPreviousATCSector']])

temp_df = temp_df[temp_df['DurationInPreviousATCSector'] > '00:02:00']

temp_df = temp_df[~((temp_df['OriginInfo'] == 'VTBD') & (temp_df['OldCurrentRadarController'] == 'DAR21'))]

# valid_sectors = [
#     'APE1', 'APE2', 'APN', 'APS1', 'APS2',
#     'APW', 'ARR19_1', 'ARR19_2', 'DAR21',
#     'DDP21', 'DSB19'
# ]

valid_sectors = ['SECTOR_1N',
                 'SECTOR_1S',
                 'SECTOR_2N',
                 'SECTOR_2S',
                 'SECTOR_3N',
                 'SECTOR_3S',
                 'SECTOR_4N',
                 'SECTOR_4S',
                 'SECTOR_5N',
                 'SECTOR_5S',
                 'SECTOR_6N',
                 'SECTOR_6S', ]

temp_df.to_csv("/Users/pongabha/Desktop/temp_df.csv")

df_conflict = pd.DataFrame()

for event in change_watch_dict:
    if ~(change_watch_dict[event].find("CONFLICT")) == -1:
        temp_df = dict_of_df[change_watch_dict[event]]
        temp_df.rename(columns={"StartTime": "Time"})
        temp_df = pd_convert_time(temp_df, 2019, 12)
        temp_df = pd_conflict(temp_df, event)
        df_conflict = pd.concat([df_conflict, temp_df]).sort_values(by="Time")

sector_change_df = pd_convert_time(sector_change_df, 2019, 12)
level_change_df = pd_convert_time(level_change_df, 2019, 12)

df_sector_entry = pd_sector_entry(sector_change_df)
df_sector_exit = pd_sector_exit(sector_change_df)
df_level_change = pd_level_change(level_change_df)

# Combine entry and exit events
df_workload = pd.concat([df_sector_entry, df_sector_exit, df_level_change, df_conflict]).sort_values(by="Time")

# Compute rolling sum per sector (1-hour window, updated every minute)
df_workload_rolling = (
    df_workload.groupby("Sector")["Workload"]
    .rolling('1h', min_periods=1)
    .sum()
    .reset_index()
)

# Save results
df_workload_rolling.to_csv("/Users/pongabha/Desktop/df_combined_workload.csv")

# **Plot: Combined Workload Over Time**
fig_combined = px.line(df_workload_rolling,
                       x="Time",
                       y="Workload",
                       color="Sector",
                       title="Combined Sector Workload Over Time (1h Rolling Sum)",
                       labels={"Workload": "Rolling Workload (1h)", "Time": "Time"},
                       template="plotly_white")

# Customize the grid and time axis
fig_combined.update_layout(
    xaxis=dict(
        type="date",
        gridcolor="grey",
        gridwidth=0.5,
        dtick=3600000,  # Minor ticks every hour
        tickformat="%H:%M",  # Show hour:minute format
    ),
    yaxis=dict(
        gridcolor="grey",
        gridwidth=1.0
    ),
    plot_bgcolor="white"
)

# Show the interactive plot
fig_combined.show()

# Compute rolling sum per sector (1-hour window, updated every minute)
df_entry_rolling = (
    df_sector_entry.groupby("Sector")["Count"]
    .rolling('1h', min_periods=1)
    .sum()
    .reset_index()
)

df_exit_rolling = (
    df_sector_exit.groupby("Sector")["Count"]
    .rolling('1h', min_periods=1)
    .sum()
    .reset_index()
)

process_sector_workload(df_workload_rolling, df_entry_rolling)

#plot_sector_selectable(df_workload_rolling, df_entry_rolling)

# Save results
df_entry_rolling.to_csv(f"{output_filepath}df_sector_entry_count.csv")
df_exit_rolling.to_csv(f"{output_filepath}df_sector_exit_count.csv")
