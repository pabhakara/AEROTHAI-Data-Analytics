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

conn_postgres_source = psycopg2.connect(user="pongabhaab",
                                             password="pongabhaab2",
                                             host="172.16.129.241",
                                             port="5432",
                                             database="aerothai_dwh",
                                             options="-c search_path=dbo,sur_air")

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

year = '2024'
month = '11'
day = '22'

airport_list = "('VTBD','VTBS')"

with (conn_postgres_source):
    cursor_postgres_source = conn_postgres_source.cursor(cursor_factory=psycopg2.extras.DictCursor)
    # Create an SQL query that selects the list of flights that we want plot
    # from the source PostgreSQL database
    postgres_sql_text = f"SELECT DISTINCT t.flight_key, f.actype " \
                            f"FROM sur_air.cat062_{year}{month}{day} t " \
                            f"LEFT JOIN flight_data.flight_{year}{month} f " \
                            f"ON t.flight_id = f.id " \
                            f"WHERE (f.dep IN {airport_list} AND f.dest LIKE '%') " \
                            f"AND f.flight_key LIKE '%' \n"\
                            f"AND f.frule LIKE '%' "\
                            f"AND f.flight_key IN \n "\
                            f"(SELECT flight_key FROM ( (SELECT s.flight_key, f.actype, f.first_flevel,ROUND(s.measured_fl) as measured_fl, COUNT(*) \n " \
                            f"FROM sur_air.cat062_{year}{month}{day} s, flight_data.flight_{year}{month} f \n  " \
                            f"WHERE s.flight_key = f.flight_key\n " \
                            f"AND (f.dep LIKE 'VTBS' OR f.dep LIKE 'VTBD') \n " \
                            f"AND f.frule LIKE 'I' AND (f.op_type = 'S') \n " \
                            f"AND NOT s.measured_fl IS NULL \n " \
                            f"AND NOT (LEFT(f.first_flevel,2) LIKE 'F1%' OR LEFT(f.first_flevel,1) LIKE 'S%' OR LEFT(f.first_flevel,1) LIKE 'A%') \n " \
                            f"AND s.sector LIKE 'BANGKOK%'  AND s.measured_fl > 50 AND s.measured_fl < 160 \n " \
                            f"GROUP BY s.flight_key, f.actype, f.first_flevel, ROUND(s.measured_fl)) \n " \
                            f"ORDER BY s.flight_key ASC, COUNT(*) DESC ) a \n " \
                            f"WHERE count > 12*1-1) " \
                            f"ORDER BY t.flight_key ASC; "
    cursor_postgres_source.execute(postgres_sql_text)

    record = cursor_postgres_source.fetchall()

    summary_df = pd.DataFrame(record, columns=['flight_key', 'actype'])

    flight_key_list_with_level_off = list(summary_df['flight_key'])

    postgres_sql_text = f"SELECT DISTINCT t.flight_key, f.actype " \
                        f"FROM sur_air.cat062_{year}{month}{day} t " \
                        f"LEFT JOIN flight_data.flight_{year}{month} f " \
                        f"ON t.flight_id = f.id " \
                        f"WHERE (f.dep IN {airport_list} AND f.dest LIKE '%') " \
                        f"AND f.flight_key LIKE '%' \n" \
                        f"AND f.frule LIKE '%' " \
                        f"AND NOT f.flight_key IN \n " \
                        f"(SELECT flight_key FROM ( (SELECT s.flight_key, f.actype, f.first_flevel,ROUND(s.measured_fl) as measured_fl, COUNT(*) \n " \
                        f"FROM sur_air.cat062_{year}{month}{day} s, flight_data.flight_{year}{month} f \n  " \
                        f"WHERE s.flight_key = f.flight_key\n " \
                        f"AND (f.dep LIKE 'VTBS' OR f.dep LIKE 'VTBD') \n " \
                        f"AND f.frule LIKE 'I' AND (f.op_type = 'S') \n " \
                        f"AND NOT s.measured_fl IS NULL \n " \
                        f"AND NOT (LEFT(f.first_flevel,2) LIKE 'F1%' OR LEFT(f.first_flevel,1) LIKE 'S%' OR LEFT(f.first_flevel,1) LIKE 'A%') \n " \
                        f"AND s.sector LIKE 'BANGKOK%'  AND s.measured_fl > 50 AND s.measured_fl < 160 \n " \
                        f"GROUP BY s.flight_key, f.actype, f.first_flevel, ROUND(s.measured_fl)) \n " \
                        f"ORDER BY s.flight_key ASC, COUNT(*) DESC ) a \n " \
                        f"WHERE count > 12*1-1) " \
                        f"ORDER BY t.flight_key ASC; "
    cursor_postgres_source.execute(postgres_sql_text)

    record = cursor_postgres_source.fetchall()

    summary_df = pd.DataFrame(record, columns=['flight_key', 'actype'])

    flight_key_list_without_level_off = list(summary_df['flight_key'])

fig = make_subplots(specs=[[{"secondary_y": True}]])



with conn_postgres_source:

    track_duration_list = []
    track_distance_list = []

    for flight_key in flight_key_list_without_level_off:
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
                            f"ON a.airport_identifier = f.dep " \
                            f"WHERE f.flight_key = '{flight_key}' " \
                            f"AND public.ST_Intersects(public.ST_Buffer(a.geom,1),t.position) " \
                            f"AND t.measured_fl > 1 \n" \
                            f"AND f.frule = 'I' \n" \
                            f"AND f.op_type = 'S' \n" \
                            f"--AND t.sector LIKE 'BANGKOK%' \n" \
                            f"ORDER BY t.app_time "


        print(postgres_sql_text)
        cursor_postgres_source.execute(postgres_sql_text)
        record = cursor_postgres_source.fetchall()
        if len(record) > 0:
            df = pd.DataFrame(record, columns=['flight_key','app_time','sector','dist_from_last_position','measured_fl',
                                           'final_state_selected_alt_dap','latitude','longitude'])


            print(df)

            accumulated_distance = 0
            accumulated_distance_list = []

            accumulated_time = 0
            accumulated_time_list = []

            for k in range(len(df)):
                accumulated_distance = accumulated_distance + df['dist_from_last_position'][k]
                accumulated_time = df['app_time'][k] - df['app_time'][0]
                accumulated_distance_list.append(accumulated_distance)
                accumulated_time_list.append(accumulated_time)

            df['accumulated_distance'] = pd.DataFrame(accumulated_distance_list,columns =['accumulated_distance'])
            df['accumulated_time'] = pd.DataFrame(accumulated_time_list, columns=['accumulated_time'])

            track_duration_list.append(df.iloc[-1,-1])
            track_distance_list.append(df.iloc[-1,-2])

            my_color = "rgba(0, 0, 0"

            fig.add_trace(go.Line(name=f"{flight_key}",
                                  x=(df["accumulated_distance"]),
                                  y=(df["measured_fl"]*100),
                                  line=dict(color=my_color+',0.05)',width=1)
                                  ),
                         secondary_y=False,
                          )

    track_duration_list = []
    track_distance_list = []

    for flight_key in flight_key_list_with_level_off:
        # for mode_a in mode_a_list:
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
                            f"ON a.airport_identifier = f.dep " \
                            f"WHERE f.flight_key = '{flight_key}' " \
                            f"AND public.ST_Intersects(public.ST_Buffer(a.geom,1),t.position) " \
                            f"AND t.measured_fl > 1 \n" \
                            f"AND f.frule = 'I' \n" \
                            f"AND f.op_type = 'S' \n" \
                            f"--AND t.sector LIKE 'BANGKOK%' \n" \
                            f"ORDER BY t.app_time "

        print(postgres_sql_text)
        cursor_postgres_source.execute(postgres_sql_text)
        record = cursor_postgres_source.fetchall()
        if len(record) > 0:
            df = pd.DataFrame(record,
                              columns=['flight_key', 'app_time', 'sector', 'dist_from_last_position', 'measured_fl',
                                       'final_state_selected_alt_dap', 'latitude', 'longitude'])

            print(df)

            accumulated_distance = 0
            accumulated_distance_list = []

            accumulated_time = 0
            accumulated_time_list = []

            for k in range(len(df)):
                accumulated_distance = accumulated_distance + df['dist_from_last_position'][k]
                accumulated_time = df['app_time'][k] - df['app_time'][0]
                accumulated_distance_list.append(accumulated_distance)
                accumulated_time_list.append(accumulated_time)

            df['accumulated_distance'] = pd.DataFrame(accumulated_distance_list, columns=['accumulated_distance'])
            df['accumulated_time'] = pd.DataFrame(accumulated_time_list, columns=['accumulated_time'])

            track_duration_list.append(df.iloc[-1, -1])
            track_distance_list.append(df.iloc[-1, -2])

            my_color = "rgba(255, 0, 0"

            fig.add_trace(go.Line(name=f"{flight_key}",
                                  x=(df["accumulated_distance"]),
                                  y=(df["measured_fl"] * 100),
                                  line=dict(color=my_color + ',0.2)', width=1)
                                  ),
                          secondary_y=False,
                          )


    # Add figure title
    fig.update_layout(
            title_text=f"Climb Profiles of Flights Departing from {airport_list} on {year}-{month}-{day}",
            # title_text=f"Vertical Profile"
            )

    # Set x-axis title
    fig.update_xaxes(title_text="Accumulated Distance (NM)")

    # Set y-axes titles
    # fig.update_yaxes(title_text="<b>Measured FL (ft)</b>", secondary_y=False)
    fig.update_yaxes(title_text=f"<b>Altitude (ft)</b>", secondary_y=False)

    fig.update_layout(
        xaxis=dict(
            rangeslider=dict(
                visible=True
            ),
            type="-"
        )
    )


    fig.write_html(f"/Users/pongabha/Dropbox/Workspace/AEROTHAI Data Analytics/CCO-CDO Analysis/VTBS-Climb-Profile "
                   f"-{year}{month}{day}.html")
    df.to_csv(f"/Users/pongabha/Dropbox/Workspace/AEROTHAI Data Analytics/CCO-CDO Analysis/VTBS-Climb-Profile "
                   f"-{year}{month}{day}.csv")

    fig.show()


summary_df['track_duration'] = pd.DataFrame(track_duration_list, columns=['track_duration'])
summary_df['track_distance'] = pd.DataFrame(track_distance_list, columns=['track_distance'])

print(summary_df)

