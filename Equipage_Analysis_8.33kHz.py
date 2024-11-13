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

# conn_postgres = psycopg2.connect(user="postgres",
#                                  password="password",
#                                  host="localhost",
#                                  port="5432",
#                                  database="temp",
#                                  options="-c search_path=dbo," + schema_name)
#

filter_old = {
    "8.33 kHz": "(item10_cns like '%Y%/%')",
    "No 8.33 kHz": "NOT(item10_cns like '%Y%/%')",
}

filter_new = {
    "8.33 kHz": "(comnav like '%Y%%')",
    "No 8.33 kHz": "NOT(comnav like '%Y%')",
}


analysis = "8.33 kHz"


date_list = pd.date_range(start='2013-01-01', end='2024-09-30',freq='M')

equipage_list = filter_old.keys()
equipage_count_df = pd.DataFrame()
with conn_postgres:
    for equipage in equipage_list:
        # year_list = ['2023','2022','2021', '2020', '2019', '2018', '2017', '2016', '2015', '2014', '2013']
        # month_list = ['01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12']
        equipage_count_temp_3 = pd.DataFrame()
        for date in date_list:
            year = f"{date.year}"
            month = f"{date.month:02d}"
            print(f"{year}-{month}")
            equipage_count_temp_2 = pd.DataFrame([f"{year}-{month}"], columns=['time'])

            if date.year < 2024:
                # postgres_sql_text = f"SELECT '{year}_{month}','{equipage}',dest,count(*) " \
                postgres_sql_text = f"SELECT count(*) " \
                                    f"FROM {schema_name}.\"{year}_{month}_fdmc\" " \
                                    f"WHERE {filter_old[equipage]} " \
                                    f"and dest like '%'" \
                                    f"and frule like 'I';" \
                    # f"GROUP BY dest;"
            else:
                postgres_sql_text = f"SELECT count(*) " \
                                    f"FROM {schema_name}.\"flight_{year}{month}\" " \
                                    f"WHERE {filter_new[equipage]} " \
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

        equipage_count_temp_4 = pd.DataFrame()
        # year_list = ['2023']
        # month_list = ['01','02','03','04','05','06','07']
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
#     go.Bar(name = "8.33Hz",
#            x=df.index,
#            y=df['8.33Hz'],
#            offsetgroup=0),
#     go.Bar(name = "No 8.33Hz",
#            x=df.index,
#            y=df['No 8.33Hz'],
#            offsetgroup=1),
#     go.Line(name="Percentage",
#                x=df.index,
#                y=df['8.33Hz']/(df['No 8.33Hz']+df['8.33Hz']),
#             ),
#     ]
# )
# Create figure with secondary y-axis
fig = make_subplots(specs=[[{"secondary_y": True}]])

# Add traces
fig.add_trace(
    go.Bar(name="8.33 kHz",
                      x=df.index,
                      y=df['8.33 kHz'],
                      offsetgroup=0),
    secondary_y=False,
)

fig.add_trace(
    go.Bar(name="No 8.33 kHz",
                      x=df.index,
                      y=df['No 8.33 kHz'],
                      offsetgroup=1),
    secondary_y=False,
)

fig.add_trace(
    go.Line(name="8.33Hz %",
                           x=df.index,
                           y=df['8.33 kHz']/(df['No 8.33 kHz']+df['8.33 kHz'])*100,
            line=dict(color="#808080")
                        ),
    secondary_y=True,
)

# Add figure title
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