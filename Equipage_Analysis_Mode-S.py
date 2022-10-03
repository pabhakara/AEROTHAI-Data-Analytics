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
                                 password="pongabhaab",
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
#     "Mode-S": "(item10_cns like '%/%L%' "
#                  "or item10_cns like '%/%E%' "
#                  "or item10_cns like '%/%H%' "
#                  "or item10_cns like '%/%S%' "
#                  "or item10_cns like '%/%P%' "
#                  "or item10_cns like '%/%I%' "
#                  "or item10_cns like '%/%X%') ",
#     "No Mode-S": "NOT (item10_cns like '%/%L%' "
#                  "or item10_cns like '%/%E%' "
#                  "or item10_cns like '%/%H%' "
#                  "or item10_cns like '%/%S%' "
#                  "or item10_cns like '%/%P%' "
#                  "or item10_cns like '%/%I%' "
#                  "or item10_cns like '%/%X%') "
# }

# filter = {
#     "ADS-B":"(item10_cns like '%/%B%'or item10_cns like '%/%U%' or item10_cns like '%/%V%') ",
# }
date_list = pd.date_range(start='2013-01-01', end='2022-09-25',freq='M')

equipage_list = list(filter.keys())
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

            # postgres_sql_text = f"SELECT '{year}_{month}','{equipage}',dest,count(*) " \
            postgres_sql_text = f"SELECT count(*) " \
                                f"FROM {schema_name}.\"{year}_{month}_fdmc\" " \
                                f"WHERE {filter[equipage]} " \
                                f"and dest like '%'" \
                                f"and frule like 'I';" \
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

# Add figure title
fig.update_layout(
    title_text="Historical Monthly IFR Movements with Mode-S (January 2013 to August 2022)"
)

# Set x-axis title
fig.update_xaxes(title_text="Time")

# Set y-axes titles
fig.update_yaxes(title_text="<b>Number of Movements</b>", secondary_y=False)
fig.update_yaxes(title_text=f"<b>Percentage</b>", secondary_y=True)

fig.show()

# Set title
# fig.update_layout(
#     title_text="Monthly IFR Movements in Bangkok FIR  with Mode-S Equipage (January 2013 to June 2022)"
# )



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
fig.write_html(f"/Users/pongabha/Dropbox/Workspace/AEROTHAI Data Analytics/Equipage Analysis/Mode-S.html")
df.to_csv(f"/Users/pongabha/Dropbox/Workspace/AEROTHAI Data Analytics/Equipage Analysis/Mode-S.csv")
#fig.write_image("/Users/pongabha/Desktop/ADS-B.png")
fig.show()