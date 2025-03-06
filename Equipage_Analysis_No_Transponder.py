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

schema_name = 'flight_data'
conn_postgres = psycopg2.connect(user="pongabhaab",
                                 password="pongabhaab2",
                                 host="172.16.129.241",
                                 port="5432",
                                 database="aerothai_dwh",
                                 options="-c search_path=dbo," + schema_name)

# conn_postgres = psycopg2.connect(user = "postgres",
#                                   password = "password",
#                                   host = "127.0.0.1",
#                                   port = "5432",
#                                   database = "temp",
#                                   options="-c search_path=dbo,public")

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

filter_old = {
    "Mode-S EHS": "(item10_cns like '%/%L%' "
                  "or item10_cns like '%/%H%' ) ",
    "Mode-S ELS": "(item10_cns like '%/%S%' "
                  "or item10_cns like '%/%E%' "
                  "or item10_cns like '%/%P%' "
                  "or item10_cns like '%/%I%' "
                  "or item10_cns like '%/%X%') ",
    "Mode-C": " item10_cns like '%/%C%'"
              "  AND (item10_cns like '%/%L%' "
                 "or item10_cns like '%/%E%' "
                 "or item10_cns like '%/%H%' "
                 "or item10_cns like '%/%S%' "
                 "or item10_cns like '%/%P%' "
                 "or item10_cns like '%/%I%' "
                 "or item10_cns like '%/%X%') ",
    "Mode-A": " item10_cns like '%/%A%'"
              "  AND (item10_cns like '%/%L%' "
                 "or item10_cns like '%/%E%' "
                 "or item10_cns like '%/%H%' "
                 "or item10_cns like '%/%S%' "
                 "or item10_cns like '%/%P%' "
                 "or item10_cns like '%/%I%' "
                 "or item10_cns like '%/%X%') ",
    "No-Transponder": " item10_cns like '%/%A%'"
              "  AND (item10_cns like '%/%L%' "
              "or item10_cns like '%/%E%' "
              "or item10_cns like '%/%H%' "
              "or item10_cns like '%/%S%' "
              "or item10_cns like '%/%P%' "
              "or item10_cns like '%/%I%' "
              "or item10_cns like '%/%X%') "
}

filter_new = {
    "Mode-S EHS": "(sur like '%L%' "
                  "or sur like '%H%' ) ",
    "Mode-S ELS": "(sur like '%S%' "
                  "or sur like '%E%' "
                  "or sur like '%P%' "
                  "or sur like '%I%' "
                  "or sur like '%X%') ",
    "No Mode-S": "NOT (sur like '%L%' "
                 "or sur like '%E%' "
                 "or sur like '%H%' "
                 "or sur like '%S%' "
                 "or sur like '%P%' "
                 "or sur like '%I%' "
                 "or sur like '%X%') "
}

analysis = f"Mode-S"

date_list = pd.date_range(start='2019-07-01', end='2025-02-28',freq='M')

equipage_list = list(filter_old.keys())
equipage_count_df = pd.DataFrame()

color_list = ['#636EFA',
     '#EF553B',
     '#00CC96',
     '#AB63FA',
     '#FFA15A',
     '#19D3F3',
     '#FF6692',
     '#B6E880',
     '#FF97FF',
     '#FECB52']

with conn_postgres:
    for equipage in equipage_list:
        equipage_count_temp_3 = pd.DataFrame()
        for date in date_list:
            year = f"{date.year}"
            month = f"{date.month:02d}"
            day = f"{date.day:02d}"
            print(f"{year}-{month}")
            equipage_count_temp_2 = pd.DataFrame([f"{year}-{month}"], columns=['time'])

            if date.year < 2024:
                # postgres_sql_text = f"SELECT '{year}_{month}','{equipage}',dest,count(*) " \
                postgres_sql_text = f"SELECT count(*) " \
                                    f"FROM {schema_name}.\"{year}_{month}_fdmc\" " \
                                    f"WHERE {filter_old[equipage]} " \
                                    f"and dest like '%'" \
                                    f"and frule like '%';"
            else:
                postgres_sql_text = f"SELECT count(*) " \
                                    f"FROM {schema_name}.\"flight_{year}{month}\" " \
                                    f"WHERE {filter_new[equipage]} " \
                                    f"and dest like '%'" \
                                    f"and frule like '%';"
                # f"GROUP BY dest;"
            cursor_postgres = conn_postgres.cursor()
            cursor_postgres.execute(postgres_sql_text)
            record = cursor_postgres.fetchall()
            # print(equipage)
            equipage_count_temp = pd.DataFrame([record[0][0]], columns=[equipage])
            equipage_count_temp_2 = pd.concat([equipage_count_temp_2, equipage_count_temp], axis=1)
            equipage_count_temp_2 = equipage_count_temp_2.set_index('time')
            equipage_count_temp_3 = pd.concat([equipage_count_temp_3, equipage_count_temp_2])

        equipage_count_df[equipage] = equipage_count_temp_3
df = equipage_count_df

print(df)

# Create figure with secondary y-axis
fig = make_subplots(specs=[[{"secondary_y": True}]])

# Add traces
fig.add_trace(
    go.Bar(name=equipage_list[0],
                      x=df.index,
                      y=df[equipage_list[0]],
                      offsetgroup=0),
    secondary_y=False,
)

fig.add_trace(
    go.Bar(name=equipage_list[1],
                      x=df.index,
                      y=df[equipage_list[1]],
                      offsetgroup=1),
    secondary_y=False,
)

fig.add_trace(
    go.Bar(name=equipage_list[2],
                      x=df.index,
                      y=df[equipage_list[2]],
                      offsetgroup=2),
    secondary_y=False,
)

fig.add_trace(
    go.Line(name="Mode-S EHS %",
                           x=df.index,
                           y=df['Mode-S EHS']/(df['Mode-S EHS']+df['Mode-S ELS']+df['No Mode-S'])*100,
            line=dict(color="#808080")
                        ),
    secondary_y=True,
)

fig.add_trace(
    go.Line(name="No Mode-S %",
                           x=df.index,
                           y=df['No Mode-S']/(df['Mode-S EHS']+df['Mode-S ELS']+df['No Mode-S'])*100,
            line=dict(color="#1DCA1D")
                        ),
    secondary_y=True,
)

fig.update_layout(
    title_text=f"Historical Monthly IFR/VFR Movements with {analysis} Capability " \
               f"{date_list[0].month_name()} {date_list[0].year} to " \
               f"{date_list[-1].month_name()} {date_list[-1].year} "
)

# Set x-axis title
fig.update_xaxes(title_text="Time")

# Set y-axes titles
fig.update_yaxes(title_text="<b>Number of Movements</b>", secondary_y=False)
fig.update_yaxes(title_text=f"<b>Percentage</b>", secondary_y=True)

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
fig.write_html(f"/Users/pongabha/Dropbox/Workspace/AEROTHAI Data Analytics/Equipage Analysis/{analysis} "
               f"{date_list[0].year}-{date_list[0].month:02d} To "
               f"{date_list[-1].year}-{date_list[-1].month:02d}"
               f".html")
df.to_csv(f"/Users/pongabha/Dropbox/Workspace/AEROTHAI Data Analytics/Equipage Analysis/{analysis} "
          f"{date_list[0].year}-{date_list[0].month:02d} To "
          f"{date_list[-1].year}-{date_list[-1].month:02d}"
          f".csv")
#fig.write_image("/Users/pongabha/Desktop/ADS-B.png")
fig.show()