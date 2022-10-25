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


# conn_postgres_source = psycopg2.connect(user="pongabhaab",
#                                              password="pongabhaab",
#                                              host="172.16.129.241",
#                                              port="5432",
#                                              database="aerothai_dwh",
#                                              options="-c search_path=dbo,sur_air")

conn_postgres_source = psycopg2.connect(user="postgres",
                                             password="password",
                                             host="localhost",
                                             port="5432",
                                             database="temp",
                                             options="-c search_path=dbo,sur_air")

output_filepath = '/Users/pongabha/Dropbox/Workspace/AEROTHAI Data Analytics/Flight_Proflie_Plots/'
files = glob.glob(f"{output_filepath}*")
for f in files:
    os.remove(f)

year = '2022'
month = '10'
day = '16'

#STAR_list = ['LEBIM','NORTA','EASTE','WILLA','DOLNI']
STAR_list = ['%']

with conn_postgres_source:
    cursor_postgres_source = conn_postgres_source.cursor(cursor_factory=psycopg2.extras.DictCursor)
    # Create an SQL query that selects the list of flights that we want plot
    # from the source PostgreSQL database
    for STAR in STAR_list:
        postgres_sql_text = f"SELECT DISTINCT t.flight_key, f.actype, '{STAR}' " \
                            f"FROM sur_air.cat062_{year}{month}{day} t " \
                            f"LEFT JOIN flight_data.flight_{year}{month} f " \
                            f"ON t.flight_id = f.id " \
                            f"WHERE (f.dep LIKE '%' AND f.dest LIKE '%') " \
                            f"AND f.item15_route LIKE '%{STAR}%' " \
                            f"AND t.acid LIKE '%TLM601%' " \
                            f"AND f.frule LIKE '%'; "
        cursor_postgres_source.execute(postgres_sql_text)
        print(postgres_sql_text)

        record = cursor_postgres_source.fetchall()
        print(record)
        summary_df = pd.DataFrame(record, columns=['flight_key', 'actype','star'])

    flight_key_list = list(summary_df['flight_key'])

#print(flight_key_list)

fig = make_subplots(specs=[[{"secondary_y": True}]])

track_duration_list = []
track_distance_list = []

with conn_postgres_source:

    for flight_key in flight_key_list:
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
                            f"ORDER BY t.app_time "
                            # f"AND public.ST_WITHIN(t.\"2d_position\",public.ST_Buffer(a.geom,2)) " \

        #print(postgres_sql_text)
        cursor_postgres_source.execute(postgres_sql_text)
        record = cursor_postgres_source.fetchall()
        df = pd.DataFrame(record, columns=['flight_key','app_time','sector','dist_from_last_position','measured_fl',
                                           'final_state_selected_alt_dap','latitude','longitude'])

        #print(df)

        accumulated_distance = 0
        accumulated_distance_list = []

        accumulated_time = 0
        accumulated_time_list = []

        for k in range(len(df)):
            accumulated_distance = accumulated_distance + df['dist_from_last_position'][k]
            accumulated_time = df['app_time'][k] -  df['app_time'][0]
            #print(accumulated_distance)
            #print(df['dist_from_last_position'][k])
            accumulated_distance_list.append(accumulated_distance)
            accumulated_time_list.append(accumulated_time)

        df['accumulated_distance'] = pd.DataFrame(accumulated_distance_list,columns =['accumulated_distance'])
        df['accumulated_time'] = pd.DataFrame(accumulated_time_list, columns=['accumulated_time'])

        track_duration_list.append(df.iloc[-1,-1])
        track_distance_list.append(df.iloc[-1,-2])

        #print(df)
        #
        # fig = make_subplots(
        #     rows=1, cols=1,
        #     # specs=[[{"type": "scatter"}, {"type": "mapbox"},{"type": "scatter3d"}]])
        #     # specs=[[{"type": "scatter"}, {"type": "mapbox"},{"type": "scatter3d"}]])

        # fig = make_subplots(specs=[[{"secondary_y": True}]])

        #fig = go.Figure()

        fig.add_trace(go.Line(name=f"{flight_key}",
                              x=(df["accumulated_distance"]),
                              y=(df["measured_fl"]*100)),
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

        # fig.add_trace(go.Scattermapbox(lat=df["latitude"], lon=df["longitude"],
        #               mode='markers',
        #               marker_size=14),
        #               row=1, col=2)

        # fig.add_trace(go.Scatter3d(x = df['longitude'], y = df['latitude'], z = df['measured_fl'],
        #               mode='markers',-
        #               marker_size=14),
        #               row=1, col=3)
        #
        # fig.update_layout(height=500, width=700,
        #                   title_text="Multiple Subplots with Titles")

        # fig.update_layout(
        #     autosize=True,
        #     hovermode='closest',
        #     mapbox=dict(
        #         style='open-street-map',
        #         domain={'x': [0, 0.4], 'y': [0, 1]},
        #         bearing=0,
        #         center=dict(
        #             lat=45,
        #             lon=-73
        #         ),
        #         pitch=0,
        #         zoom=5
        #     ),
        #     mapbox2=dict(
        #         style='open-street-map',
        #         domain={'x': [0.6, 1.0], 'y': [0, 1]},
        #         bearing=0,
        #         center=dict(
        #             lat=45,
        #             lon=-73
        #         ),
        #         pitch=0,
        #         zoom=5
        #     ),
        #     mapbox3=dict(
        #         style='open-street-map',
        #         domain={'x': [0.6, 1.0], 'y': [0, 1]},
        #         bearing=0,
        #         center=dict(
        #             lat=45,
        #             lon=-73
        #         ),
        #         pitch=0,
        #         zoom=5
        #     ),
        # )

        # fig = make_subplots(
        #     rows=1, cols=2, subplot_titles=('Montreal 1', 'Montreal 2'),
        #     specs=[[{"type": "mapbox"}, {"type": "mapbox"}]]
        # )
        #
        # fig.add_trace(go.Scattermapbox(
        #     lat=[45.5017],
        #     lon=[-73.5673],
        #     mode='markers',
        #     marker_size=14,
        #     text=['Montreal 1'],
        # ), 1, 1)

        # fig = px.line
        # fig.show()
        #
        # fig = px.scatter_mapbox(df, lat="latitude", lon="longitude",
        #                         color_continuous_scale=px.colors.sequential.Viridis,
        #                         color="measured_fl",
        #                         hover_data=["app_time", "measured_fl"],
        #                         zoom=5)
        # # fig = px.scatter_mapbox(us_cities, lat="lat", lon="lon", hover_name="City", hover_data=["State", "Population"],
        # #                         color_discrete_sequence=["fuchsia"], )
        # fig.update_layout(mapbox_style="carto-darkmatter")
        # fig.update_layout(margin={"r": 0, "t": 0, "l": 0, "b": 0})
        # fig.update_layout(title=flight_key[0])
        # fig.update_traces(marker=dict(size=3))
        # fig.show()
        #
        #
        #
        # fig = px.scatter_3d(df, x='longitude', y='latitude', z='measured_fl',
        #                     color='sector')

    # Add range slider
    # fig.update_layout(
    #     xaxis=dict(
    #         rangeselector=dict(
    #             buttons=list([
    #                 dict(count=1,
    #                      label="1m",
    #                      step="month",
    #                      stepmode="backward"),
    #                 dict(count=6,
    #                      label="6m",
    #                      step="month",
    #                      stepmode="backward"),
    #                 dict(count=1,
    #                      label="YTD",
    #                      step="year",
    #                      stepmode="todate"),
    #                 dict(count=1,
    #                      label="1y",
    #                      step="year",
    #                      stepmode="backward"),
    #                 dict(step="all")
    #             ])
    #         ),
    #         rangeslider=dict(
    #             visible=True
    #         ),
    #         type="date"
    #     )
    # )

    fig.show()


        # fig, ax = plt.subplots()
        #
        # #ax.plot(df['app_time'], df['measured_fl'], color='b')
        # ax.plot(accumulated_distance_df['accumulated_distance'], df['measured_fl'], color='b')
        # # ax.plot(df['app_time'], df['entry'], color='b', label='entry_count')
        # # ax.plot(df['app_time'], df['workload'], color='r', label='workload')
        # # df = px.data.gapminder().query("continent=='Oceania'")
        #
        # #ax.set_xlabel('Time (UTC)')
        # ax.set_xlabel('Distance Travelled (NM)')
        # #ax.set_ylabel('Measured FL', color='k')
        # plt.xticks(rotation=90)
        #
        # #date_form = DateFormatter("%Y-%m-%d %H:%M")
        # #ax.xaxis.set_major_formatter(date_form)
        #
        # #ax.xaxis.set_major_locator(MultipleLocator(1/60/6))
        # ax.xaxis.set_major_locator(MultipleLocator(20))
        # ax.yaxis.set_major_locator(MultipleLocator(20))
        #
        # # Change minor ticks to show every 5. (20/4 = 5)
        # #ax.xaxis.set_minor_locator(AutoMinorLocator(1/60/12))
        # ax.xaxis.set_minor_locator(AutoMinorLocator(5))
        # ax.yaxis.set_minor_locator(AutoMinorLocator(1))
        # ax.set_ylim([0,460])
        # ax.set_xlim([0,accumulated_distance_list[-1]])
        #
        # plt.title(f"{flight_key[0]}")
        # plt.xticks(rotation=90)
        # #plt.legend()
        # plt.grid()
        # plt.tight_layout()
        # plt.savefig(f"{output_filepath}{flight_key[0]}.png")
summary_df['track_duration'] = pd.DataFrame(track_duration_list, columns=['track_duration'])
summary_df['track_distance'] = pd.DataFrame(track_distance_list, columns=['track_distance'])

print(summary_df)

