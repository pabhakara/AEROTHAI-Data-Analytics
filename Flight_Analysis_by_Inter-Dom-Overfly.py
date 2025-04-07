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
#
# conn_postgres = psycopg2.connect(user="postgres",
#                                  password="password",
#                                  host="localhost",
#                                  port="5432",
#                                  database="temp",
#                                  options="-c search_path=dbo," + schema_name)

# # filter = {
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
        "International": "(ftype = 1 OR ftype = 2)",
        "Domestic": " (ftype = 4)",
        "Overfly": "(ftype = 3)"
}

filter_new = {
        "International": "(ftype = 1 OR ftype = 2)",
        "Domestic": " (ftype = 4)",
        "Overfly": "(ftype = 3)"
}

# filter_old = {
#         "Commercial": "(op_type = 'S' OR op_type = 'N' )",
#         "Military": " (op_type = 'M')",
#         "General": "(op_type = 'G')"
# }
#
# filter_new = {
#         "Commercial": "(op_type = 'S' OR op_type = 'N' )",
#         "Military": " (op_type = 'M')",
#         "General": "(op_type = 'G')"
# }



analysis = "Inter-Dom-Overfly"

dest = '%'
dep = '%'
condition = 'OR'

date_list = pd.date_range(start='2013-01-01', end='2025-02-28',freq='M')
print(date_list)

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
                                    f"and (dest like '{dest}'" \
                                    f"{condition} dep like '{dep}')" \
                                    f"and frule like 'I';" \
                    # f"GROUP BY dest;"
            else:
                postgres_sql_text = f"SELECT count(*) " \
                                    f"FROM {schema_name}.\"flight_{year}{month}\" " \
                                    f"WHERE {filter_new[equipage]} " \
                                    f"and (dest like '{dest}'" \
                                    f"{condition} dep like '{dep}')" \
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

# Convert index to datetime for proper date operations
df.index = pd.to_datetime(df.index)

# Save the original monthly totals
df_total = df.copy()

# Add number of days in each month
df['days_in_month'] = df.index.days_in_month

# Calculate daily averages
df_avg = df_total.copy()
for col in ['International', 'Domestic', 'Overfly']:
    df_avg[col] = df_total[col] / df['days_in_month']

# Drop helper column
df.drop(columns='days_in_month', inplace=True)

# Create figure with secondary y-axis
fig = make_subplots(specs=[[{"secondary_y": True}]])

# Add bar traces for monthly total flights
fig.add_trace(
    go.Bar(name="International (Total)",
           x=df_total.index,
           y=df_total['International'],
           offsetgroup=0),
    secondary_y=False,
)

fig.add_trace(
    go.Bar(name="Domestic (Total)",
           x=df_total.index,
           y=df_total['Domestic'],
           offsetgroup=1),
    secondary_y=False,
)

fig.add_trace(
    go.Bar(name="Overfly (Total)",
           x=df_total.index,
           y=df_total['Overfly'],
           offsetgroup=2),
    secondary_y=False,
)

# Add dotted lines for daily average flights
fig.add_trace(
    go.Scatter(name="International Avg/Day",
               x=df_avg.index,
               y=df_avg['International'],
               line=dict(dash='dot', color="#0008FF")),
    secondary_y=False,
)

fig.add_trace(
    go.Scatter(name="Domestic Avg/Day",
               x=df_avg.index,
               y=df_avg['Domestic'],
               line=dict(dash='dot', color="#FF0000")),
    secondary_y=False,
)

fig.add_trace(
    go.Scatter(name="Overfly Avg/Day",
               x=df_avg.index,
               y=df_avg['Overfly'],
               line=dict(dash='dot', color="#008700")),
    secondary_y=False,
)

# Add percentage lines (same as before)
fig.add_trace(
    go.Scatter(name="International %",
               x=df_total.index,
               y=(df_total['International'] / df_total.sum(axis=1)) * 100,
               line=dict(color="#0008FF")),
    secondary_y=True,
)

fig.add_trace(
    go.Scatter(name="Domestic %",
               x=df_total.index,
               y=(df_total['Domestic'] / df_total.sum(axis=1)) * 100,
               line=dict(color="#FF0000")),
    secondary_y=True,
)

fig.add_trace(
    go.Scatter(name="Overfly %",
               x=df_total.index,
               y=(df_total['Overfly'] / df_total.sum(axis=1)) * 100,
               line=dict(color="#008700")),
    secondary_y=True,
)

# Add figure title and labels
fig.update_layout(
    title_text=f"Historical Monthly IFR Movements from/to {dest} by {analysis}<br>"
               f"{date_list[0].month_name()} {date_list[0].year} to "
               f"{date_list[-1].month_name()} {date_list[-1].year}<br>"
               f"(Bars = Monthly Total | Dotted Lines = Daily Avg | Solid Lines = % Share)"
)

fig.update_xaxes(title_text="Time")
fig.update_yaxes(title_text="<b>Number of Flights</b>", secondary_y=False)
fig.update_yaxes(title_text="<b>Percentage</b>", secondary_y=True)

# Add range slider
fig.update_layout(
    xaxis=dict(
        rangeselector=dict(
            buttons=list([
                dict(count=1, label="1m", step="month", stepmode="backward"),
                dict(count=6, label="6m", step="month", stepmode="backward"),
                dict(count=1, label="YTD", step="year", stepmode="todate"),
                dict(count=1, label="1y", step="year", stepmode="backward"),
                dict(step="all")
            ])
        ),
        rangeslider=dict(visible=True),
        type="date"
    )
)

# Export to HTML and CSV
fig.write_html(f"/Users/pongabha/Dropbox/Workspace/AEROTHAI Data Analytics/Equipage Analysis/{analysis} "
               f"{date_list[0].year}-{date_list[0].month:02d} To "
               f"{date_list[-1].year}-{date_list[-1].month:02d}.html")

df_total.to_csv(f"/Users/pongabha/Dropbox/Workspace/AEROTHAI Data Analytics/Equipage Analysis/{analysis} "
                f"{date_list[0].year}-{date_list[0].month:02d} To "
                f"{date_list[-1].year}-{date_list[-1].month:02d} - Monthly Total.csv")

df_avg.to_csv(f"/Users/pongabha/Dropbox/Workspace/AEROTHAI Data Analytics/Equipage Analysis/{analysis} "
              f"{date_list[0].year}-{date_list[0].month:02d} To "
              f"{date_list[-1].year}-{date_list[-1].month:02d} - Daily Avg.csv")

fig.show()
