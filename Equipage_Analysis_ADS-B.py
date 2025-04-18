import psycopg2.extras
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import calendar
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

# conn_postgres = psycopg2.connect(user="postgres",
#                                  password="password",
#                                  host="localhost",
#                                  port="5432",
#                                  database="temp",
#                                  options="-c search_path=dbo," + schema_name)

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

# filter = {
#     "Mode-S EHS": "(item10_cns like '%/%L%' "
#                   "or item10_cns like '%/%H%' ) ",
#     "Mode-S ELS": "(item10_cns like '%/%S%' "
#                   "or item10_cns like '%/%E%' "
#                   "or item10_cns like '%/%P%' "
#                   "or item10_cns like '%/%I%' "
#                   "or item10_cns like '%/%X%') ",
#     "No Mode-S": "NOT (item10_cns like '%/%L%' "
#                  "or item10_cns like '%/%E%' "
#                  "or item10_cns like '%/%H%' "
#                  "or item10_cns like '%/%S%' "
#                  "or item10_cns like '%/%P%' "
#                  "or item10_cns like '%/%I%' "
#                  "or item10_cns like '%/%X%') "
# }

filter_old = {
    "ADS-B":"(item10_cns like '%/%B%') ",
    "No ADS-B": "NOT (item10_cns like '%/%B%') ",
}

filter_new = {
    "ADS-B":"(sur like '%B%') ",
    "No ADS-B": "NOT (sur like '%B%') ",
}
analysis = "ADS-B"

date_list = pd.date_range(start='2013-01-01', end='2025-01-31',freq='M')

equipage_list = filter_old.keys()
equipage_count_df = pd.DataFrame()
with conn_postgres:
    for equipage in equipage_list:
        equipage_count_temp_3 = pd.DataFrame()
        for date in date_list:
            year = f"{date.year}"
            month = f"{date.month:02d}"
            print(f"{year}-{month}")
            equipage_count_temp_2 = pd.DataFrame([f"{year}-{month}"], columns=['time'])
            # postgres_sql_text = f"SELECT '{year}_{month}','{equipage}',dest,count(*) " \

            if date.year < 2024:
                postgres_sql_text = f"SELECT count(*) " \
                                    f"FROM {schema_name}.\"{year}_{month}_fdmc\" " \
                                    f"WHERE {filter_old[equipage]} " \
                                    f"and dest like '%'" \
                                    f"and frule like 'I';"
            else:
                postgres_sql_text = f"SELECT count(*) " \
                                    f"FROM {schema_name}.\"flight_{year}{month}\" " \
                                    f"WHERE {filter_new[equipage]} " \
                                    f"and dest like '%'" \
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

        equipage_count_temp_4 = pd.DataFrame()
        # year_list = ['2023']
        # month_list = ['01','02','03','04','05','06','07','08','09','10']
        # for year in year_list:
        #     for month in month_list:
        #         print(f"{year}-{month}")
        #         equipage_count_temp_2 = pd.DataFrame([f"{year}-{month}"], columns=['time'])
        #
        #         # postgres_sql_text = f"SELECT '{year}_{month}','{equipage}',dest,count(*) " \
        #         postgres_sql_text = f"SELECT count(*) " \
        #                             f"FROM {schema_name}.\"{year}_{month}_fdmc\" " \
        #                             f"WHERE {filter[equipage]} " \
        #                             f"and dest like '%'" \
        #                             f"and frule like 'I';" \
        #             # f"GROUP BY dest;"
        #         cursor_postgres = conn_postgres.cursor()
        #         cursor_postgres.execute(postgres_sql_text)
        #         record = cursor_postgres.fetchall()
        #         # print(equipage)
        #         equipage_count_temp = pd.DataFrame([record[0][0]], columns=[equipage])
        #         equipage_count_temp_2 = pd.concat([equipage_count_temp_2, equipage_count_temp], axis=1)
        #         equipage_count_temp_2 = equipage_count_temp_2.set_index('time')
        #         equipage_count_temp_4 = pd.concat([equipage_count_temp_4, equipage_count_temp_2])
        equipage_count_df[equipage] = pd.concat([equipage_count_temp_3, equipage_count_temp_4])
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
    go.Bar(name="ADS-B",
                      x=df.index,
                      y=df['ADS-B'],
                      offsetgroup=0),
    secondary_y=False,
)

fig.add_trace(
    go.Bar(name="No ADS-B",
                      x=df.index,
                      y=df['No ADS-B'],
                      offsetgroup=1),
    secondary_y=False,
)

fig.add_trace(
    go.Line(name="ADS-B %",
                           x=df.index,
                           y=df['ADS-B']/(df['No ADS-B']+df['ADS-B'])*100,
            line=dict(color="#808080")
                        ),
    secondary_y=True,
)

# Add figure title
fig.update_layout(
    title_text=f"Historical Monthly IFR Movements with ADS-B Capability " \
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
fig.write_html(f"/Users/pongabha/Dropbox/Workspace/AEROTHAI Data Analytics/Equipage Analysis/{analysis}-"
               f"{date_list[0].year}-{date_list[0].month:02d} To "
               f"{date_list[-1].year}-{date_list[-1].month:02d}"
               f".html")
df.to_csv(f"/Users/pongabha/Dropbox/Workspace/AEROTHAI Data Analytics/Equipage Analysis/{analysis}-"
          f"{date_list[0].year}-{date_list[0].month:02d} To "
          f"{date_list[-1].year}-{date_list[-1].month:02d}"
          f".csv")
#fig.write_image("/Users/pongabha/Desktop/ADS-B.png")
fig.show()