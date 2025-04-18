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
# conn_postgres = psycopg2.connect(user="pongabhaab",
#                                  password="pongabhaab2",
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

analysis = "PBN with D-D or D-D-I"

filter_old_data = {
    "RNAV 5 (D/D)": "(pbn_type like '%B1%' OR pbn_type like '%B3%')" ,
    "RNAV 5 (D/D/I)": "(pbn_type like '%B1%' OR pbn_type like '%B4%')",
    "RNAV 2 (D/D)": "(pbn_type like '%C1%' OR pbn_type like '%C3%')" ,
    "RNAV 2 (D/D/I)": "(pbn_type like '%C1%' OR pbn_type like '%C4%')",
    "RNAV 1 (D/D)": "(pbn_type like '%D1%' OR pbn_type like '%D3%')" ,
    "RNAV 1 (D/D/I)": "(pbn_type like '%D1%' OR pbn_type like '%D4%')",
    "Total": "(pbn_type like '%')",
}

filter_new_data = {
    "RNAV 5 (D/D)": "(item18_json->>'PBN' like '%B1%' OR item18_json->>'PBN' like '%B3%')" ,
    "RNAV 5 (D/D/I)": "(item18_json->>'PBN' like '%B1%' OR item18_json->>'PBN' like '%B4%')",
    "RNAV 2 (D/D)": "(item18_json->>'PBN' like '%C1%' OR item18_json->>'PBN' like '%C3%')",
    "RNAV 2 (D/D/I)": "(item18_json->>'PBN' like '%C1%' OR item18_json->>'PBN' like '%C4%')",
    "RNAV 1 (D/D)": "(item18_json->>'PBN' like '%D1%' OR item18_json->>'PBN' like '%D3%')",
    "RNAV 1 (D/D/I)": "(item18_json->>'PBN' like '%D1%' OR item18_json->>'PBN' like '%D4%')",
    "Total": "(item18_json->>'PBN' like '%')",
}

filter = filter_new_data

date_list = pd.date_range(start='2019-07-01', end='2025-01-31',freq='M')

equipage_list = list(filter.keys())
equipage_count_df = pd.DataFrame()
with conn_postgres:
    for equipage in equipage_list:
        equipage_count_temp_3 = pd.DataFrame()
        for date in date_list:
            year = f"{date.year}"
            month = f"{date.month:02d}"
            equipage_count_temp_2 = pd.DataFrame([f"{year}-{month}"], columns=['time'])

            if date.year < 2024:
                # postgres_sql_text = f"SELECT '{year}_{month}','{equipage}',dest,count(*) " \
                postgres_sql_text = f"SELECT count(*) " \
                                    f"FROM {schema_name}.\"{year}_{month}_radar\" " \
                                    f"WHERE {filter_old_data[equipage]} " \
                                    f"and (dest LIKE '%') " \
                                    f"and frule like 'I';" \
                        # f"GROUP BY dest;"
            else:
                # postgres_sql_text = f"SELECT '{year}_{month}','{equipage}',dest,count(*) " \
                postgres_sql_text = f"SELECT count(*) " \
                                    f"FROM {schema_name}.\"flight_{year}{month}\" " \
                                    f"WHERE {filter_new_data[equipage]} " \
                                    f"and (dest LIKE '%') " \
                                    f"and frule like 'I';" \
                    # f"GROUP BY dest;"

            print(postgres_sql_text)
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

color_list = ['#636EFA',
     '#EF553B',
     '#00CC96',
     '#AB63FA',
     '#FFA15A',
     '#19D3F3',
     '#FF6692',
     '#B6E880',
     '#FF97FF',
     '#FECB52',
     '#636EFA',
     '#EF553B']
# Add traces
for k in range(0,6):
    fig.add_trace(
        go.Bar(name=equipage_list[k],
                          x=df.index,
                          y=df[equipage_list[k]],
                          offsetgroup=k,
                          marker_color=color_list[k]),
        secondary_y=False,
    )

for k in range(0,6):
    fig.add_trace(
        go.Line(name=f"{equipage_list[k]} as % of Total IFR",
                          x=df.index,
                          y=df[equipage_list[k]]/df[equipage_list[-1]]*100,
                          marker_color=color_list[k]),
        secondary_y=True,
    )

fig.update_layout(
    title_text=f"Historical Monthly IFR Movements with {analysis} Capability " \
               f"{date_list[0].month_name()} {date_list[0].year} to " \
               f"{date_list[-1].month_name()} {date_list[-1].year} "
)

# Set x-axis title
fig.update_xaxes(title_text="Time")

# Set y-axes titles
fig.update_yaxes(title_text="<b>Number of Movements</b>", secondary_y=False)
fig.update_yaxes(title_text=f"<b>Percentage</b>", secondary_y=True)

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