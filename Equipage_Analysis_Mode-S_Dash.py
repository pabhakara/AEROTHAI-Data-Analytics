import psycopg2.extras
import pandas as pd
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
#     "ADS-B":"(item10_cns like '%/%B%'or item10_cns like '%/%U%' or item10_cns like '%/%V%') ",
# }

equipage_list = filter.keys()
equipage_count_df = pd.DataFrame()
with conn_postgres:
    for equipage in equipage_list:
        year_list = ['2021', '2020', '2019', '2018', '2017', '2016', '2015', '2014', '2013']
        month_list = ['01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12']
        for year in reversed(year_list):
            for month in month_list:
                print(f"{year}-{month}")
                postgres_sql_text = f"SELECT '{year}_{month}','{equipage}',dest,count(*) " \
                                    f"FROM {schema_name}.\"{year}_{month}_fdmc\" " \
                                    f"WHERE {filter[equipage]} " \
                                    f"and dest like 'VT%'" \
                                    f"and frule like 'I'" \
                                    f"GROUP BY dest;"
                #postgres_sql_text = f"SELECT '{year}-{month}','{equipage}',count(*) " \
                cursor_postgres = conn_postgres.cursor()
                cursor_postgres.execute(postgres_sql_text)
                record = cursor_postgres.fetchall()
                equipage_count_temp = pd.DataFrame(record, columns=['time', 'equipage', 'dest', 'count'])
                #equipage_count_temp = pd.DataFrame(record, columns=['time', 'equipage', 'count'])
                equipage_count_df = (pd.concat([equipage_count_df, equipage_count_temp], ignore_index=True))

        year_list = ['2022']
        month_list = ['01', '02', '03', '04', '05', '06']
        for year in year_list:
            for month in month_list:
                print(f"{year}-{month}")
                #postgres_sql_text = f"SELECT '{year}-{month}','{equipage}',count(*) " \
                postgres_sql_text = f"SELECT '{year}_{month}','{equipage}',dest,count(*) " \
                                    f"FROM {schema_name}.\"{year}_{month}_fdmc\" " \
                                    f"WHERE {filter[equipage]} " \
                                    f"and dest like 'VT%'" \
                                    f"and frule like 'I'" \
                                    f"GROUP BY dest;"
                cursor_postgres = conn_postgres.cursor()
                cursor_postgres.execute(postgres_sql_text)
                record = cursor_postgres.fetchall()
                equipage_count_temp = pd.DataFrame(record, columns=['time', 'equipage', 'dest', 'count'])
                #equipage_count_temp = pd.DataFrame(record, columns=['time', 'equipage', 'count'])
                equipage_count_df = (pd.concat([equipage_count_df, equipage_count_temp], ignore_index=True))
print(equipage_count_df)

# fig = px.bar(equipage_count_df.sort_values(['time', 'equipage'],
#                                            ascending=[True, True]),
#              x='time', y='count', color='equipage')
# fig.update_layout(title_text="IFR Movements with ADS-B Equipage (2013-2021)")
# fig.show()


# -------------------------------------------------------------------------------------

app = dash.Dash(__name__)

# -------------------------------------------------------------------------------------
app.layout = html.Div([

    html.Div([
        html.Pre(children="Equipage Statistics",
                 style={"text-align": "center", "font-size": "100%", "color": "black"})
    ]),

    html.Div([
        html.Label(['X-axis categories to compare:'], style={'font-weight': 'bold'}),
        dcc.RadioItems(
            id='xaxis_raditem',
            options=[
                {'label': 'Year-Month ', 'value': 'time'},
                {'label': 'Equipage ', 'value': 'equipage'},
                {'label': 'Destination Airport ', 'value': 'dest'}
            ],
            value='time',
            style={"width": "50%"}
        ),
    ]),

    html.Div([
        html.Br(),
        html.Label(['Y-axis values to compare:'], style={'font-weight': 'bold'}),
        dcc.RadioItems(
            id='yaxis_raditem',
            options=[
                {'label': 'Time Spent on Site (hours)', 'value': 'count'}
            ],
            value='count',
            style={"width": "50%"}
        ),
    ]),

    html.Div([
        dcc.Graph(id='the_graph')
    ]),

])


# -------------------------------------------------------------------------------------
@app.callback(
    Output(component_id='the_graph', component_property='figure'),
    [Input(component_id='xaxis_raditem', component_property='value'),
     Input(component_id='yaxis_raditem', component_property='value')]
)
def update_graph(x_axis, y_axis):
    dff = equipage_count_df
    # print(dff[[x_axis,y_axis]][:1])

    barchart = px.bar(
        data_frame=dff,
        x=x_axis,
        y=y_axis,
        title=y_axis + ': by ' + x_axis,
        # facet_col='Borough',
        color='equipage',
        barmode='group',
    )
    barchart.update_layout(title={'xanchor': 'center', 'yanchor': 'top', 'y': 0.9, 'x': 0.5, })

    # barchart.update_layout(xaxis={'categoryorder': 'total ascending'},
    #                        title={'xanchor': 'center', 'yanchor': 'top', 'y': 0.9, 'x': 0.5, })

    return (barchart)


if __name__ == '__main__':
    app.run_server(debug=True)
