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


# equipage_filter = {
#     "ADS-B": "(item10_cns like '%/%B%')",
#     "All": "(item10_cns like '%') "
# }

equipage_filter = {
        "GLS": "(item10_cns like '%A%/%')",
        "No GLS": "NOT (item10_cns like '%A%/%')"
}

airport_list = ['VTBS','VTBD','VTSP','VTCC','VTSM','VTSB','VTSS','VTSG','VTPP','VTSF','VTUU','VTUD','%']
#airport_list = ['%']

equipage_list = list(equipage_filter.keys())
print(equipage_list)
equipage_count_df = pd.DataFrame()
with conn_postgres:
    year_list = ['2023','2022','2021', '2020', '2019', '2018', '2017', '2016', '2015', '2014', '2013']
    month_list = ['01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12']
    # year_list = []
    # month_list = []
    equipage_count_temp_3 = pd.DataFrame()
    for year in reversed(year_list):
        for month in month_list:
            print(f"{year}-{month}")
            equipage_count_temp_2 = pd.DataFrame([f"{year}-{month}"], columns=['time'])
            for equipage in equipage_list:
                for airport in airport_list:
                    # postgres_sql_text = f"SELECT '{year}_{month}','{equipage}',dest,count(*) " \
                    postgres_sql_text = f"SELECT count(*) " \
                                        f"FROM {schema_name}.\"{year}_{month}_fdmc\" " \
                                        f"WHERE {equipage_filter[equipage]} " \
                                        f"and (dest like '{airport}') " \
                                        f"and frule like 'I';" \
                        # f"GROUP BY dest;"
                    cursor_postgres = conn_postgres.cursor()
                    cursor_postgres.execute(postgres_sql_text)
                    record = cursor_postgres.fetchall()
                    equipage_count_temp = pd.DataFrame([record[0][0]],columns=[airport])
                    equipage_count_temp_2 = pd.concat([equipage_count_temp_2, equipage_count_temp],axis=1)
            equipage_count_temp_2 = equipage_count_temp_2.set_index('time')
            equipage_count_temp_3 = pd.concat([equipage_count_temp_3, equipage_count_temp_2])
    # year_list = ['2023']
    # month_list = ['01','02','03','04','05','06','07']
    #month_list = ['08']
    equipage_count_temp_4 = pd.DataFrame()
    # for year in year_list:
    #     for month in month_list:
    #         print(f"{year}-{month}")
    #         equipage_count_temp_2 = pd.DataFrame([f"{year}-{month}"], columns=['time'])
    #         for equipage in equipage_list:
    #             for airport in airport_list:
    #                 postgres_sql_text = f"SELECT count(*) " \
    #                                     f"FROM {schema_name}.\"{year}_{month}_fdmc\" " \
    #                                     f"WHERE {equipage_filter[equipage]} " \
    #                                     f"and (dest like '{airport}' or dep like '{airport}') " \
    #                                     f"and frule like 'I';" \
    #                     # f"GROUP BY dest;"
    #                 cursor_postgres = conn_postgres.cursor()
    #                 cursor_postgres.execute(postgres_sql_text)
    #                 record = cursor_postgres.fetchall()
    #                 #print(equipage)
    #                 equipage_count_temp = pd.DataFrame([record[0][0]],columns=[airport])
    #                 equipage_count_temp_2 = pd.concat([equipage_count_temp_2, equipage_count_temp],axis=1)
    #         equipage_count_temp_2 = equipage_count_temp_2.set_index('time')
    #         equipage_count_temp_4 = pd.concat([equipage_count_temp_4, equipage_count_temp_2])
    #         #print(equipage_count_temp_4)
    equipage_count_df = pd.concat([equipage_count_temp_3, equipage_count_temp_4])
print(equipage_count_df)

df = equipage_count_df

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

# Create figure
fig = go.Figure()
k = 0
for airport in airport_list:
    # fig.add_trace(go.Bar(name = airport,
    # x=df.index,
    # y=df[airport],
    # offsetgroup= k))
    fig.add_trace(go.Scatter(name=airport,
                             x=df.index,
                             y=df[airport]))
    # fig.add_trace(go.Bar(name = airport,
    # x=df.index,
    # y=df[airport]))
    k = k + 1

fig.update_xaxes(
    dtick="M1",
    tickformat="%b-%Y")

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

fig.update_layout(
    title_text="Monthly IFR Movements with GLS at Some VT Airports (January 2013 to December 2023)"
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

path = '/Users/pongabha/Library/CloudStorage/Dropbox/Workspace/AEROTHAI Data Analytics/Equipage Analysis'

fig.write_html(f"{path}/PBN_by_airport.html")
fig.show()

#print(equipage_count_df)
df = equipage_count_df.transpose()
print(df)
df.to_csv(f"{path}/PBN_by_airport.csv")