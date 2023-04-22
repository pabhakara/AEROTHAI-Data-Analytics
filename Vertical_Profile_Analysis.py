import psycopg2
import psycopg2.extras
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.ticker import (AutoMinorLocator, MultipleLocator)
from matplotlib.dates import DateFormatter
import os
import glob
import openai

import plotly.express as px
from plotly.subplots import make_subplots
import plotly.graph_objects as go
from plotly.graph_objs.scatter.marker import Line

from dash import Dash, html, dcc
import time
import datetime as dt


def none_to_null(etd):
    if etd == 'None':
        x = 'null'
    else:
        x = "'" + etd + "'"
    return x


conn_postgres_source = psycopg2.connect(user="pongabhaab",
                                             password="pongabhaab",
                                             host="172.16.129.241",
                                             port="5432",
                                             database="aerothai_dwh",
                                             options="-c search_path=dbo,sur_air")

# conn_postgres_source = psycopg2.connect(user="postgres",
#                                         password="password",
#                                         host="localhost",
#                                         port="5432",
#                                         database="temp",
#                                         options="-c search_path=dbo,sur_air")

output_filepath = '/Users/pongabha/Dropbox/Workspace/AEROTHAI Data Analytics/Flight_Proflie_Plots/'
files = glob.glob(f"{output_filepath}*")
for f in files:
    os.remove(f)

year = '2022'
month = '10'
day = '24'

STAR_list = ['LEBIM', 'NORTA', 'EASTE', 'WILLA', 'DOLNI']
#STAR_list = ['']
summary_df = pd.DataFrame()

dest = 'VTBS'
dep = '%'

with conn_postgres_source:
    cursor_postgres_source = conn_postgres_source.cursor(cursor_factory=psycopg2.extras.DictCursor)
    # Create an SQL query that selects the list of flights that we want plot
    # from the source PostgreSQL database
    for STAR in STAR_list:
        postgres_sql_text = f"SELECT DISTINCT t.flight_key, f.actype, '{STAR}' " \
                            f"FROM sur_air.cat062_{year}{month}{day} t " \
                            f"LEFT JOIN flight_data.flight_{year}{month} f " \
                            f"ON t.flight_id = f.id " \
                            f"WHERE (f.dep LIKE '{dep}' AND f.dest LIKE '{dest}') " \
                            f"AND f.item15_route LIKE '%{STAR}%' " \
                            f"AND t.acid LIKE 'THA%' " \
                            f"AND f.frule LIKE 'I%'" \
                            f"ORDER BY t.flight_key; "
        #print(postgres_sql_text)
        cursor_postgres_source.execute(postgres_sql_text)
        record = cursor_postgres_source.fetchall()
        #print(record)
        summary_df_temp = pd.DataFrame(record, columns=['flight_key', 'actype', 'star'])
        summary_df = summary_df.append(summary_df_temp,ignore_index=True)

    flight_key_list = list(summary_df['flight_key'])
    actype_list = list(summary_df['actype'])

# print(flight_key_list)
# print(summary_df)

track_duration_list = []
track_distance_list = []
radius = 70

with conn_postgres_source:
    for flight_key in flight_key_list:
        fig = make_subplots(specs=[[{"secondary_y": True}]])
        # print(flight_key)
        cursor_postgres_source = conn_postgres_source.cursor(cursor_factory=psycopg2.extras.DictCursor)
        # Create an SQL query that selects surveillance targets based on the list of selected flights
        # from the source PostgreSQL database
        postgres_sql_text = f"SELECT t.flight_key, t.app_time, t.sector, t.dist_from_last_position, t.measured_fl \n" \
                            f", t.final_state_selected_alt_dap, t.selected_altitude_dap \n" \
                            f", t.sector \n" \
                            f", t.latitude, t.longitude \n" \
                            f" FROM airac_current.airports_buffer b, \n" \
                            f" sur_air.cat062_{year}{month}{day} t \n" \
                            f" LEFT JOIN flight_data.flight_{year}{month} f \n" \
                            f" ON t.flight_id = f.id \n" \
                            f" LEFT JOIN airac_current.airports a \n" \
                            f" ON a.airport_identifier = f.dest \n" \
                            f" WHERE f.flight_key = '{flight_key}' \n" \
                            f" AND b.airport_identifier LIKE '{dest}' \n" \
                            f" AND public.ST_Within(t.\"position\", b.nm{radius}) \n" \
                            f" ORDER BY t.app_time "
        # f"AND public.ST_WITHIN(t.\"2d_position\",public.ST_Buffer(a.geom,2)) " \
        print(postgres_sql_text)
        cursor_postgres_source.execute(postgres_sql_text)
        record = cursor_postgres_source.fetchall()
        df = pd.DataFrame(record, columns=['flight_key', 'app_time', 'sector', 'dist_from_last_position', 'measured_fl',
                                           'final_state_selected_alt_dap', 'selected_altitude_dap','sector',
                                           'latitude', 'longitude'])

        accumulated_distance = 0
        accumulated_distance_list = []

        accumulated_time = 0
        accumulated_time_list = []

        for k in range(len(df)):
            accumulated_distance = accumulated_distance + df['dist_from_last_position'][k]
            accumulated_time = (df['app_time'][k] - df['app_time'][0]).total_seconds() / 60
            # print(accumulated_distance)
            # print(df['dist_from_last_position'][k])
            accumulated_distance_list.append(accumulated_distance)
            accumulated_time_list.append(accumulated_time)
        print(df)
        if df.empty:
            track_duration_list.append(['0'])
            track_distance_list.append(['0'])
        else:
            df['accumulated_distance'] = pd.DataFrame(accumulated_distance_list, columns=['accumulated_distance'])
            df['accumulated_time'] = pd.DataFrame(accumulated_time_list, columns=['accumulated_time'])

            track_distance = df.iloc[-1, -2]
            track_duration = df.iloc[-1, -1]

            track_duration_list.append(track_duration)
            track_distance_list.append(track_distance)

            # fig = go.Figure()

            #fig.add_trace(go.Line(name=f"{flight_key} {track_distance:.2f}",
            fig.add_trace(go.Line(name=f"Measured FL",
                                  x=(df["accumulated_distance"]),
                                  y=(df["measured_fl"] * 100)),
                          secondary_y=False,
                          )
            fig.add_trace(go.Line(name="FSS Altitude",
                                  x=df["accumulated_distance"],
                                  y=df["final_state_selected_alt_dap"]),
                          secondary_y=False,
                          )
            fig.add_trace(go.Line(name="Selected Altitude",
                                  x=df["accumulated_distance"],
                                  y=df["selected_altitude_dap"]),
                          secondary_y=False,
                          )

            # Add figure title
            fig.update_layout(
                title_text=f"Vertical Profile of Flight {flight_key} Type:{actype_list[0]}"
                #title_text=f"Vertical Profile"
            )

            # Set x-axis title
            #fig.update_xaxes(title_text=f"<b>Accumulated Time (Minutes)</b>")
            fig.update_xaxes(title_text=f"<b>Accumulated Distance (NM)</b>")

            # Set y-axes titles
            # fig.update_yaxes(title_text="<b>Measured FL (ft)</b>", secondary_y=False)
            fig.update_yaxes(title_text=f"<b>Altitude (ft)</b>", secondary_y=False)
            #fig.show()
            fig.write_image(f"{output_filepath}{flight_key}.png")
            fig.write_html(f"{output_filepath}{flight_key}.html")

print(track_distance_list)

print(summary_df.reset_index())

summary_df['track_duration'] = pd.DataFrame(track_duration_list, columns=['track_duration'])
summary_df['track_distance'] = pd.DataFrame(track_distance_list, columns=['track_distance'])

summary_df = summary_df.set_index('flight_key')

print(summary_df)

summary_df.to_csv(f"{output_filepath}VTBS_arrivals_stats.csv")

fig = px.histogram(summary_df, x="track_distance",color="star",nbins=100)
fig.show()
