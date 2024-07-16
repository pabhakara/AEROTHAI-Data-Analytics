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

pd.options.plotting.backend = "plotly"

schema_name = 'flight_data'
conn_postgres = psycopg2.connect(user="pongabhaab",
                                 password="pongabhaab2",
                                 host="172.16.129.241",
                                 port="5432",
                                 database="aerothai_dwh",
                                 options="-c search_path=dbo," + schema_name)

# filter = {
#     "Mode-S aircraft identification":"(item10_cns like '%/%S%' "
#                                      "or item10_cns like '%/%L%' "
#                                      "or item10_cns like '%/%E%' "
#                                      "or item10_cns like '%/%H%' "
#                                      "or item10_cns like '%/%I%') ",
#     "Mode-S pressure-altitude": "(item10_cns like '%/%L%' "
#                                 "or item10_cns like '%/%E%' "
#                                 "or item10_cns like '%/%S%' "
#                                 "or item10_cns like '%/%H%'"
#                                 "or item10_cns like '%/%P%') ",
#     "Mode-S extended squitter": "(item10_cns like '%/%L%' "
#                                 "or item10_cns like '%/%E%' ) ",
#     "Mode-S enhanced surveillance": "(item10_cns like '%/%L%' "
#                                     "or item10_cns like '%/%H%' ) ",
#     "No Mode-S": "NOT(item10_cns like '%/%L%' "
#                  "or item10_cns like '%/%E%' "
#                  "or item10_cns like '%/%S%' "
#                  "or item10_cns like '%/%H%'"
#                  "or item10_cns like '%/%P%'"
#                  "or item10_cns like '%/%I%' "
#                  "or item10_cns like '%/%X%')"
# }

filter = {
    "Mode-S EHS": "(item10_cns like '%/%L%' "
                  "or item10_cns like '%/%H%' ) ",
    "Mode-S ELS": "(item10_cns like '%/%S%' "
                  "or item10_cns like '%/%E%' "
                  "or item10_cns like '%/%P%' "
                  "or item10_cns like '%/%I%' "
                  "or item10_cns like '%/%X%') ",
    "No Mode-S": "NOT (item10_cns like '%/%L%' "
                 "or item10_cns like '%/%E%' "
                 "or item10_cns like '%/%H%' "
                 "or item10_cns like '%/%S%' "
                 "or item10_cns like '%/%P%' "
                 "or item10_cns like '%/%I%' "
                 "or item10_cns like '%/%X%') "
}

# filter = {
#     "ADS-B":"(item10_cns like '%/%B%'or item10_cns like '%/%U%' or item10_cns like '%/%V%') ",
# }

date_list = pd.date_range(start='2013-01-01', end='2022-09-25',freq='M')

equipage_list = filter.keys()
equipage_count_df = pd.DataFrame()
with conn_postgres:
    for equipage in equipage_list:
        year_list = ['2021', '2020', '2019', '2018', '2017', '2016', '2015', '2014', '2013']
        month_list = ['01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12']
        for date in date_list:
            year = f"{date.year}"
            month = f"{date.month:02d}"
            print(f"{year}-{month}")
            # postgres_sql_text = f"SELECT '{year}_{month}','{equipage}',dest,count(*) " \
            postgres_sql_text = f"SELECT '{year}-{month}',dest,count(*) " \
                                f"FROM {schema_name}.\"{year}_{month}_fdmc\" " \
                                f"WHERE {filter[equipage]} " \
                                f"and dest like '%'" \
                                f"and frule like 'I' " \
                                f"GROUP BY dest;"
            cursor_postgres = conn_postgres.cursor()
            cursor_postgres.execute(postgres_sql_text)
            record = cursor_postgres.fetchall()
            # equipage_count_temp = pd.DataFrame(record, columns=['year_month', 'equipage', 'dest', 'count'])
            equipage_count_temp = pd.DataFrame(record, columns=['time', 'dest', 'count'])
            equipage_count_df = (pd.concat([equipage_count_df, equipage_count_temp], ignore_index=True))

        # year_list = ['2022']
        # month_list = ['01','02','03','04','05','06']
        # for year in year_list:
        #     for month in month_list:
        #         print(f"{year}-{month}")
        #         # postgres_sql_text = f"SELECT '{year}_{month}','{equipage}',dest,count(*) " \
        #         postgres_sql_text = f"SELECT '{year}-{month}',dest,count(*) " \
        #                             f"FROM {schema_name}.\"{year}_{month}_fdmc\" " \
        #                             f"WHERE {filter[equipage]} " \
        #                             f"and dest like '%'" \
        #                             f"and frule like 'I' " \
        #                             f"GROUP BY dest;"
        #         cursor_postgres = conn_postgres.cursor()
        #         cursor_postgres.execute(postgres_sql_text)
        #         record = cursor_postgres.fetchall()
        #         # equipage_count_temp = pd.DataFrame(record, columns=['year_month', 'equipage', 'dest', 'count'])
        #         # equipage_count_temp = pd.DataFrame(record, columns=['time', 'equipage', 'count'])
        #         equipage_count_temp = pd.DataFrame(record, columns=['time', 'dest', 'count'])
        #         equipage_count_df = (pd.concat([equipage_count_df, equipage_count_temp], ignore_index=True))
#print(equipage_count_df)
df = equipage_count_df
# df = equipage_count_df.query('equipage == "Mode-S EHS"')
# fig = px.bar(equipage_count_df.sort_values(['time', 'equipage'],
#                                            ascending=[True, True]),
#              x='time', y='count', color='equipage')
# fig.update_layout(title_text="IFR Movements with Mode-S Equipage (2013-2021)")
# fig.show()

print(df['time'])
#print(df['count'])

# Create figure
fig = go.Figure(
    data=[
    go.Bar(name = "VTBS",
           x=df['time'],
           y=df.query('dest == "VTBS"')['count'],
           offsetgroup=0),
    go.Bar(name = "VTBD",
           x=df['time'],
           y=df.query('dest == "VTBD"')['count'],
           offsetgroup=1),
    go.Bar(name="VTSP",
           x=df['time'],
           y=df.query('dest == "VTSP"')['count'],
           offsetgroup=2),
    go.Bar(name="VTCC",
           x=df['time'],
           y=df.query('dest == "VTCC"')['count'],
           offsetgroup=3),
    go.Bar(name="VTSM",
           x=df['time'],
           y=df.query('dest == "VTSM"')['count'],
           offsetgroup=4),
    go.Bar(name="VTSS",
           x=df['time'],
           y=df.query('dest == "VTSS"')['count'],
           offsetgroup=5),
    ]
)

# Set title
fig.update_layout(
    title_text="Monthly IFR Movements in Bangkok FIR  with Mode-S Equipage (January 2013 to June 2022)"
)

# Add range slider
fig.update_layout(
    xaxis=dict(
        rangeselector=dict(
            buttons=list([
                dict(count=1,
                     label="1m",
                     step="month",
                     stepmode="backward"),
                dict(count=6,
                     label="6m",
                     step="month",
                     stepmode="backward"),
                dict(count=1,
                     label="YTD",
                     step="year",
                     stepmode="todate"),
                dict(count=1,
                     label="1y",
                     step="year",
                     stepmode="backward"),
                dict(step="all")
            ])
        ),
        rangeslider=dict(
            visible=True
        ),
        type="date"
    )
)
fig.write_html("/Users/pongabha/Desktop/DEST-VT.html")
fig.show()