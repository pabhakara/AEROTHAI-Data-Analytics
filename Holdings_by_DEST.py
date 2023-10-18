import psycopg2.extras
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import datetime as dt
import dash
from dash import dcc
from dash import html
from dash.dependencies import Input, Output

import plotly
import plotly.express as px

pd.options.plotting.backend = "plotly"

schema_name = 'track'
# conn_postgres = psycopg2.connect(user="pongabhaab",
#                                  password="pongabhaab",
#                                  host="172.16.129.241",
#                                  port="5432",
#                                  database="aerothai_dwh",
#                                 options="-c search_path=dbo," + schema_name)
conn_postgres = psycopg2.connect(user="postgres",
                                 password="password",
                                 host="localhost",
                                 port="5432",
                                 database="temp",
                                 options="-c search_path=dbo," + schema_name)

analysis = "Holdings"

# filter = {
#     "Holdings": "(public.ST_IsSimple(geom) IS FALSE AND frule = 'I' AND op_type ='S' AND dest LIKE 'VT%') ",
#     "Total": "frule = 'I' AND op_type ='S' AND dest LIKE 'VT%' ",
# }

date_list = pd.date_range(start='2022-05-31', end='2023-08-31', freq='M')
df = pd.DataFrame()
print(date_list)
with conn_postgres:
    for date in date_list:
        year = f"{date.year}"
        month = f"{date.month:02d}"

        # postgres_sql_text = f"SELECT '{year}_{month}','{equipage}',dest,count(*) " \
        # postgres_sql_text = f"SELECT count(*) " \
        #                     f"FROM {schema_name}.\"{year}_{month}_fdmc\" " \
        #                     f"WHERE {filter[equipage]} " \
        #                     f"and (dest LIKE '%') " \
        #                     f"and frule like 'I';" \
        postgres_sql_text = f"SELECT '{year}_{month}' as yyyy_mm,a.dest," \
                            f"b.count as total,a.count as hold," \
                            f"ROUND((a.count/b.count::numeric*100),2) as percent " \
                            f"FROM " \
                            f"(SELECT dest, COUNT(*) " \
                            f"FROM track.track_cat62_{year}{month} " \
                            f"WHERE public.ST_IsSimple(geom) IS FALSE AND frule = 'I' AND op_type ='S' " \
                            f"AND dest LIKE 'VT%' " \
                            f"GROUP BY dest " \
                            f"ORDER BY count) a, " \
                            f"(SELECT dest, COUNT(*)  " \
                            f"FROM track.track_cat62_{year}{month} " \
                            f"WHERE frule = 'I' AND op_type ='S' " \
                            f"AND dest LIKE 'VT%' " \
                            f"GROUP BY dest " \
                            f"ORDER BY count DESC) b " \
                            f"WHERE a.dest = b.dest " \
                            f"ORDER BY total DESC "
        print(postgres_sql_text)
        cursor_postgres = conn_postgres.cursor()
        cursor_postgres.execute(postgres_sql_text)
        record = cursor_postgres.fetchall()
        df_temp = pd.DataFrame(record)
        df = pd.concat([df, df_temp])
df.to_csv(f"/Users/pongabha/Dropbox/Workspace/AEROTHAI Data Analytics/Equipage Analysis/{analysis}.csv")

# print(df)


# Create figure
# fig = go.Figure(
#     data=[
#     go.Bar(name = "ADS-B",
#            x=df.index,
#            y=df['ADS-B'],
#            offsetgroup=0),
#     go.Bar(name = "No ADS-B",
#            x=df.index,
#            y=df['No ADS-B'],
#            offsetgroup=1),
#     go.Line(name="Percentage",
#                x=df.index,
#                y=df['ADS-B']/(df['No ADS-B']+df['ADS-B']),
#             ),
#     ]
# )
# Create figure with secondary y-axis
# fig = make_subplots(specs=[[{"secondary_y": True}]])
#
# color_list = ['#636EFA',
#      '#EF553B',
#      '#00CC96',
#      '#AB63FA',
#      '#FFA15A',
#      '#19D3F3',
#      '#FF6692',
#      '#B6E880',
#      '#FF97FF',
#      '#FECB52']
#
# # Add traces
# for k in range(0,9):
#     fig.add_trace(
#         go.Bar(name=equipage_list[k],
#                           x=df.index,
#                           y=df[equipage_list[k]],
#                           offsetgroup=k,
#                           marker_color=color_list[k]),
#         secondary_y=False,
#     )
#
# for k in range(0, 9):
#     fig.add_trace(
#         go.Line(name=f"{equipage_list[k]} as % of Total IFR",
#                           x=df.index,
#                           y=df[equipage_list[k]]/df[equipage_list[-1]]*100,
#                           marker_color=color_list[k]),
#         secondary_y=True,
#     )
#     # fig.add_trace(
#     #     go.Line(name="Thai %",
#     #                            x=df.index,
#     #                            y=df[equipage_list[0]]/(df[equipage_list[0]]+df[equipage_list[1]])*100,
#     #             line=dict(color="#808080")
#     #                         ),
#     #     secondary_y=True,
#     # )
#
# # fig.add_trace(
# #     go.Bar(name=equipage_list[1],
# #                       x=df.index,
# #                       y=df[equipage_list[1]],
# #                       offsetgroup=1),
# #     secondary_y=False,
# # )
#
# # fig.add_trace(
# #     go.Bar(name=equipage_list[2],
# #                       x=df.index,
# #                       y=df[equipage_list[2]],
# #                       offsetgroup=2),
# #     secondary_y=False,
# # )
#
# # fig.add_trace(
# #     go.Line(name="Thai %",
# #                            x=df.index,
# #                            y=df[equipage_list[0]]/(df[equipage_list[0]]+df[equipage_list[1]])*100,
# #             line=dict(color="#808080")
# #                         ),
# #     secondary_y=True,
# # )
#
# # Add figure title
# fig.update_layout(
#     title_text= f"Historical Monthly IFR Movements with {analysis} by type (January 2017 to June 2023)"
# )
#
# # Set x-axis title
# fig.update_xaxes(title_text="Time")
#
# # Set y-axes titles
# fig.update_yaxes(title_text="<b>Number of Movements</b>", secondary_y=False)
# fig.update_yaxes(title_text=f"<b>Percentage</b>", secondary_y=True)
#
# fig.show()
#
# # Set title
# # fig.update_layout(
# #     title_text="Monthly IFR Movements in Bangkok FIR  with Mode-S Equipage (January 2013 to June 2022)"
# # )
#
#
#
# # Add range slider
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
# fig.write_html(f"/Users/pongabha/Dropbox/Workspace/AEROTHAI Data Analytics/Equipage Analysis/{analysis}.html")
#df.to_csv(f"/Users/pongabha/Dropbox/Workspace/AEROTHAI Data Analytics/Equipage Analysis/{analysis}.csv")
# fig.write_image("/Users/pongabha/Desktop/ADS-B.png")
# fig.show()
