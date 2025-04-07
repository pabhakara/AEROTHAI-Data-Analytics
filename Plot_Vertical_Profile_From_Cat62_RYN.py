import psycopg2
import psycopg2.extras
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.ticker import (AutoMinorLocator, MultipleLocator)
from matplotlib.dates import DateFormatter
import os
import glob

import plotly.express as px
from plotly.subplots import make_subplots
import plotly.graph_objects as go

from dash import Dash, html, dcc
import time
import datetime as dt
def none_to_null(etd):
    if etd == 'None':
        x = 'null'
    else:
        x = "'" + etd + "'"
    return x


def get_flight_key_list(conn, year: str, month: str, day: str):
    """
    Query distinct flight keys and aircraft types for a given date.

    Args:
        conn: Active psycopg2 connection object.
        year (str): Year as 'YYYY'.
        month (str): Month as 'MM'.
        day (str): Day as 'DD'.

    Returns:
        summary_df (pd.DataFrame): DataFrame with flight_key and actype.
        flight_key_list (list): List of flight_key strings.
    """
    cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

    sql = f"""
        SELECT DISTINCT t.flight_key, f.actype 
        FROM sur_air.cat062_{year}{month}{day} t 
        LEFT JOIN flight_data.flight_{year}{month} f 
        ON t.flight_id = f.id 
        WHERE f.dep LIKE 'VTBD%' 
        AND f.dest LIKE '%' 
        AND f.flight_key LIKE '%' 
        AND f.frule LIKE '%' 
        ORDER BY t.flight_key ASC;
    """

    cursor.execute(sql)
    records = cursor.fetchall()

    summary_df = pd.DataFrame(records, columns=['flight_key', 'actype'])
    flight_key_list = list(summary_df['flight_key'])

    return summary_df, flight_key_list


conn_postgres_source = psycopg2.connect(user="pongabhaab",
                                             password="pongabhaab2",
                                             host="172.16.129.241",
                                             port="5432",
                                             database="aerothai_dwh",
                                             options="-c search_path=dbo,sur_air")
#
# conn_postgres_source = psycopg2.connect(user="postgres",
#                                              password="password",
#                                              host="localhost",
#                                              port="5432",
#                                              database="temp",
#                                              options="-c search_path=dbo,sur_air")

output_filepath = '/Users/pongabha/Dropbox/Workspace/AEROTHAI Data Analytics/Flight_Proflie_Plots/'
files = glob.glob(f"{output_filepath}*")
for f in files:
    os.remove(f)

year = '2025'
month = '03'
day = '31'

with (conn_postgres_source):
    cursor_postgres_source = conn_postgres_source.cursor(cursor_factory=psycopg2.extras.DictCursor)
    # Create an SQL query that selects the list of flights that we want plot
    # from the source PostgreSQL database
    postgres_sql_text = f"SELECT DISTINCT t.flight_key, f.actype " \
                            f"FROM sur_air.cat062_{year}{month}{day} t " \
                            f"LEFT JOIN flight_data.flight_{year}{month} f " \
                            f"ON t.flight_id = f.id " \
                            f"WHERE 'RYN' = ANY(\"fdmc_bkk_fix\") AND \"item15_route\" LIKE '%W42%' "\
                            f"ORDER BY t.flight_key ASC; "
    cursor_postgres_source.execute(postgres_sql_text)

    record = cursor_postgres_source.fetchall()

    summary_df = pd.DataFrame(record, columns=['flight_key', 'actype'])

    flight_key_list = list(summary_df['flight_key'])

    #flight_key_list = get_flight_key_list(conn_postgres_source, year, month, day)

fig = make_subplots(specs=[[{"secondary_y": True}]])

track_duration_list = []
track_distance_list = []

with conn_postgres_source:

    for flight_key in flight_key_list:
    #for mode_a in mode_a_list:
        print(flight_key)
        cursor_postgres_source = conn_postgres_source.cursor(cursor_factory=psycopg2.extras.DictCursor)
        # Create an SQL query that selects surveillance targets based on the list of selected flights
        # from the source PostgreSQL database
        postgres_sql_text = f"SELECT t.flight_key, t.app_time, t.sector, t.dist_from_last_position, t.measured_fl " \
                            f", t.final_state_selected_alt_dap " \
                            f", t.latitude, t.longitude " \
                            f"FROM sur_air.cat062_{year}{month}{day} t " \
                            f"LEFT JOIN flight_data.flight_{year}{month} f " \
                            f"ON t.flight_id = f.id " \
                            f"LEFT JOIN airac_current.airports a " \
                            f"ON a.airport_identifier = f.dest " \
                            f"WHERE f.flight_key = '{flight_key}' " \
                            f"AND t.sector LIKE '%%' " \
                            f"ORDER BY t.app_time "

        print(postgres_sql_text)
        cursor_postgres_source.execute(postgres_sql_text)
        record = cursor_postgres_source.fetchall()
        if len(record) > 0:
            df = pd.DataFrame(record, columns=['flight_key','app_time','sector','dist_from_last_position','measured_fl',
                                           'final_state_selected_alt_dap','latitude','longitude'])


            print(df)

            # Coordinates of RYN fix (approximate)
            ryn_lat = 9.7775
            ryn_lon = 98.5831


            def haversine_nm(lat1, lon1, lat2, lon2):
                from math import radians, sin, cos, sqrt, atan2
                R = 3440.065  # Radius of Earth in NM
                dlat = radians(lat2 - lat1)
                dlon = radians(lon2 - lon1)
                a = sin(dlat / 2) ** 2 + cos(radians(lat1)) * cos(radians(lat2)) * sin(dlon / 2) ** 2
                return R * 2 * atan2(sqrt(a), sqrt(1 - a))


            # Accumulate and shift distance
            accumulated_distance = 0
            accumulated_distance_list = []
            accumulated_time_list = []

            ryn_distance = None
            min_dist_to_ryn = float("inf")

            for k in range(len(df)):
                dist = df['dist_from_last_position'][k]
                accumulated_distance += dist if pd.notna(dist) else 0
                accumulated_distance_list.append(accumulated_distance)

                accumulated_time = df['app_time'][k] - df['app_time'][0]
                accumulated_time_list.append(accumulated_time)

                lat, lon = df['latitude'][k], df['longitude'][k]
                if pd.notna(lat) and pd.notna(lon):
                    dist_to_ryn = haversine_nm(lat, lon, ryn_lat, ryn_lon)
                    if dist_to_ryn < min_dist_to_ryn:
                        min_dist_to_ryn = dist_to_ryn
                        ryn_distance = accumulated_distance

            # Shift accumulated distance to RYN = 0
            print(ryn_distance)
            if ryn_distance is not None:
                shifted_distance_list = [d - ryn_distance for d in accumulated_distance_list]
            else:
                shifted_distance_list = accumulated_distance_list  # fallback if RYN not found

            df['accumulated_distance'] = pd.DataFrame(shifted_distance_list, columns=['accumulated_distance'])
            df['accumulated_time'] = pd.DataFrame(accumulated_time_list, columns=['accumulated_time'])

            track_duration_list.append(df.iloc[-1,-1])
            track_distance_list.append(df.iloc[-1,-2])

            my_color = "rgba(0, 0, 0"

            fig.add_trace(go.Line(name=f"{flight_key}",
                                  x=(df["accumulated_distance"]),
                                  y=(df["measured_fl"]*100),
                                  line=dict(color=my_color+',0.1)',width=1)
                                  ),
                         secondary_y=False,
                          )

            # fig.add_trace(go.Line(name="FSS Altitude",
            #                       x=df["accumulated_distance"],
            #                       y=df["final_state_selected_alt_dap"]),
            #               secondary_y=False,
            #               )

            # Add figure title
            fig.update_layout(
                title_text=f"Vertical Profile of Flight {flight_key}"
                #title_text=f"Vertical Profile"
            )

            # Set x-axis title
            fig.update_xaxes(title_text="Accumulated Distance (NM)")

            # Set y-axes titles
            #fig.update_yaxes(title_text="<b>Measured FL (ft)</b>", secondary_y=False)
            fig.update_yaxes(title_text=f"<b>Altitude (ft)</b>", secondary_y=False)

    fig.update_layout(
        xaxis=dict(
            rangeslider=dict(
                visible=True
            ),
            type="-"
        )
    )

    fig.show()

summary_df['track_duration'] = pd.DataFrame(track_duration_list, columns=['track_duration'])
summary_df['track_distance'] = pd.DataFrame(track_distance_list, columns=['track_distance'])

print(summary_df)

