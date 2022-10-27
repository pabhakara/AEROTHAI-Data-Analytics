import psycopg2.extras
import pandas as pd
import plotly.graph_objects as go
import datetime as dt
import dash
from dash import dcc
from dash import html
from dash.dependencies import Input, Output

import plotly
import plotly.express as px

def count_target(year,month,day,filter_list,airport_list):
    equipage_count_temp_2 = pd.DataFrame({'time': [f"{year}-{month}-{day}",
                                                   f"{year}-{month}-{day}"],
                                          'dap': ['yes', 'no']})
    for filter in filter_list:
        for airport in airport_list:
            postgres_sql_text = f"SELECT count(*) "\
                                f"FROM sur_air.cat062_{year}{month}{day} t "\
                                f"LEFT JOIN flight_data.flight_{year}{month} f "\
                                f"ON t.flight_key = f.flight_key "\
                                f"LEFT JOIN airspace.tma a "\
                                f"ON t.dest = a.icaocode "\
                                f"WHERE {filter_list[filter]} " \
                                f"AND (f.dest like '{airport}') " \
                                f"AND NOT t.ias_dap IS NULL; "
            cursor_postgres = conn_postgres.cursor()
            cursor_postgres.execute(postgres_sql_text)
            record_yes = cursor_postgres.fetchall()

            postgres_sql_text = f"SELECT count(*) "\
                                f"FROM sur_air.cat062_{year}{month}{day} t "\
                                f"LEFT JOIN flight_data.flight_{year}{month} f "\
                                f"ON t.flight_key = f.flight_key "\
                                f"LEFT JOIN airspace.tma a "\
                                f"ON t.dest = a.icaocode "\
                                f"WHERE {filter_list[filter]} " \
                                f"AND (f.dest like '{airport}') " \
                                f"AND t.ias_dap IS NULL; "
            #print(postgres_sql_text)
            cursor_postgres = conn_postgres.cursor()
            cursor_postgres.execute(postgres_sql_text)
            record_no = cursor_postgres.fetchall()
            equipage_count_temp = pd.DataFrame([record_yes[0][0],record_no[0][0]],columns=[f"{airport}"])
            equipage_count_temp_2 = pd.concat([equipage_count_temp_2, equipage_count_temp], axis=1)
    return equipage_count_temp_2

pd.options.plotting.backend = "plotly"

schema_name = 'flight_data'
conn_postgres = psycopg2.connect(user="pongabhaab",
                                 password="pongabhaab",
                                 host="172.16.129.241",
                                 port="5432",
                                 database="aerothai_dwh",
                                 options="-c search_path=dbo," + schema_name)

filter_list = {
    "All": f"(t.measured_fl < a.upper/100 "
        f" AND t.measured_fl > 0 "
        f" AND public.ST_WITHIN(t.\"position\",a.geom) "
        f" AND f.frule LIKE 'I' )"
        f" AND (f.sur LIKE '%L%' OR f.sur LIKE '%H%' OR f.sur LIKE '%E%') "
        # f" ) "
}
date_list = pd.date_range(start='2022-10-23', end='2022-10-23',freq='D')

with conn_postgres:
    postgres_sql_text = f"SELECT airport_identifier FROM airac_current.airports "\
                        f"WHERE icao_code LIKE 'VT' "\
                        f"AND ifr_capability = 'Y';"
    cursor_postgres = conn_postgres.cursor()
    cursor_postgres.execute(postgres_sql_text)
    airport_list = [r[0] for r in cursor_postgres.fetchall()]

    equipage_count_df = pd.DataFrame()
    equipage_count_temp_4 = pd.DataFrame()
    for date in date_list:
        year = f"{date.year}"
        month = f"{date.month:02d}"
        day = f"{date.day:02d}"
        equipage_count_temp = count_target(year, month, day, filter_list, airport_list)
        equipage_count_temp_4 = pd.concat([equipage_count_temp_4, equipage_count_temp])
        print(f"working on {year}{month}{day}")

equipage_count_df = pd.concat([equipage_count_temp_4])
equipage_count_df = equipage_count_df.set_index(['time','dap'])
print(equipage_count_df)
df = equipage_count_df.transpose()
df.to_csv(f"/Users/pongabha/Dropbox/Workspace/Surveillance Enhancement/DAP/DAP_by_SSR_site_{year}-{month}-{day}.csv")

# # Create figure
# fig = go.Figure(
#     data=[
#     go.Bar(name = "ADS-B",
#            x=df.index,
#            y=df["ADS-B"],
#            offsetgroup=0),
#     go.Bar(name = "No ADS-B",
#            x=df.index,
#            y=df["No ADS-B"],
#            offsetgroup=1)
#     ]
# )

# # Create figure
# fig = go.Figure()
# k = 0
# for airport in airport_list:
#     # fig.add_trace(go.Bar(name = airport,
#     # x=df.index,
#     # y=df[airport],
#     # offsetgroup= k))
#     # fig.add_trace(go.Scatter(name=airport,
#     #                          x=df.index,
#     #                          y=df[airport]))
#     fig.add_trace(go.Bar(name = f"{airport}",
#     x=df.index,
#     y=df[f"{airport}"],
#     marker={'color': 'dap'})
#                   )
#     k = k + 1
#
# fig.update_xaxes(
#     dtick="D1",
#     tickformat="%d-%b-%Y")

# fig = go.Figure(
#     data=[
#     go.Bar(name = airport_list[0],
#            x=df.index,
#            y=df[airport_list[0]],
#            offsetgroup=0),
#     go.Bar(name = airport_list[1],
#            x=df.index,
#            y=df[airport_list[1]],
#            offsetgroup=1),
#     go.Bar(name = airport_list[2],
#            x=df.index,
#            y=df[airport_list[2]],
#            offsetgroup=2),
#     go.Bar(name=airport_list[3],
#            x=df.index,
#            y=df[airport_list[3]],
#            offsetgroup=3),
#     go.Bar(name=airport_list[4],
#            x=df.index,
#            y=df[airport_list[4]],
#            offsetgroup=4),
#     ]
# )
#
# updatemenu = []
# buttons = []
# # button with one option for each dataframe
# for col in df.columns:
#     buttons.append(dict(method='restyle',
#                         label=col,
#                         visible=True,
#                         args=[{'y':df[col],
#                                'x':df.index,
#                                'type':'bar'}, [0]],
#                         )
#                   )
#
# # some adjustments to the updatemenus
# updatemenu = []
# your_menu = dict()
# updatemenu.append(your_menu)
#
# updatemenu[0]['buttons'] = buttons
# updatemenu[0]['direction'] = 'down'
# updatemenu[0]['showactive'] = True
#
# # add dropdown menus to the figure
# fig.update_layout(showlegend=False, updatemenus=updatemenu)
# # # Set title
# # # fig.update_layout(
# # #     title_text="Monthly IFR Movements in Bangkok FIR  with ADS-B Capability (January 2013 to June 2022)"
# # # )

# fig.update_layout(
#     title_text="Daily Movements (Arrivals & Departures) at Some VT Airports (13-17 July 2022)"
# )

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
# fig.write_html("/Users/pongabha/Desktop/traffic_by_airport_13-17_July_2022.html")
# fig.show()