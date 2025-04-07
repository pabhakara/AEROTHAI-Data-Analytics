import psycopg2
import psycopg2.extras
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import datetime as dt
import dash
from dash import dcc, html
from dash.dependencies import Input, Output

# ---------------------------
# Configuration and Variables
# ---------------------------

schema_name = 'flight_data'
dest = '%'
dep = '%'
condition = 'OR'

# Define filters for each flight type
filter_old = {
    "International": "(ftype = 1 OR ftype = 2)",
    "Domestic": "(ftype = 4)",
    "Overfly": "(ftype = 3)"
}

filter_new = {
    "International": "(ftype = 1 OR ftype = 2)",
    "Domestic": "(ftype = 4)",
    "Overfly": "(ftype = 3)"
}

# Define the analysis period (monthly)
date_list = pd.date_range(start='2013-01-01', end='2025-02-28', freq='M')
equipage_list = list(filter_old.keys())

# ---------------------------
# Database Connection
# ---------------------------
try:
    conn_postgres = psycopg2.connect(user="pongabhaab",
                                     password="pongabhaab2",
                                     host="172.16.129.241",
                                     port="5432",
                                     database="aerothai_dwh",
                                     options="-c search_path=dbo," + schema_name)
    print("Database connection successful!")
except Exception as e:
    print(f"Error connecting to database: {e}")
    conn_postgres = None

# ---------------------------
# Build a UNION ALL Query to Fetch Data in One Go
# ---------------------------
query_parts = []
for equipage in equipage_list:
    for date in date_list:
        year = f"{date.year}"
        month = f"{date.month:02d}"
        # Choose table name and filter condition based on year
        if date.year < 2024:
            table_name = f'{schema_name}."{year}_{month}_fdmc"'
            filter_condition = filter_old[equipage]
        else:
            table_name = f'{schema_name}."flight_{year}{month}"'
            filter_condition = filter_new[equipage]

        part = f"""
        SELECT '{year}-{month}' AS time,
               '{equipage}' AS equipage,
               count(*) AS flight_count
        FROM {table_name}
        WHERE {filter_condition}
          AND (dest LIKE '{dest}' {condition} dep LIKE '{dep}')
          AND frule LIKE 'I'
        """
        query_parts.append(part.strip())

# Combine all parts with UNION ALL
final_query = " UNION ALL ".join(query_parts) + ";"

# ---------------------------
# Execute Query and Build DataFrame
# ---------------------------
if conn_postgres is not None:
    try:
        df_raw = pd.read_sql(final_query, conn_postgres)
        print("Data fetched successfully!")
    except Exception as e:
        print(f"Error executing query: {e}")
        df_raw = pd.DataFrame()
else:
    df_raw = pd.DataFrame()

# Pivot the data so that each row is a month and each column a flight type
if not df_raw.empty:
    df_total = df_raw.pivot(index='time', columns='equipage', values='flight_count')
    df_total = df_total.fillna(0)
    df_total.index = pd.to_datetime(df_total.index, format="%Y-%m")
    df_total = df_total.sort_index()
else:
    df_total = pd.DataFrame()

# Compute daily averages by dividing the monthly totals by the number of days in that month
if not df_total.empty:
    days_in_month = df_total.index.days_in_month
    df_avg = df_total.copy()
    for et in equipage_list:
        df_avg[et] = df_total[et] / days_in_month
else:
    df_avg = pd.DataFrame()

# ---------------------------
# Build Interactive Dashboard with Dash
# ---------------------------
app = dash.Dash(__name__)

app.layout = html.Div([
    html.H1("Historical Monthly IFR Movements Analysis"),
    html.P("Select Flight Types to display:"),
    dcc.Dropdown(
        id="flight_type_dropdown",
        options=[{"label": et, "value": et} for et in equipage_list],
        value=equipage_list,  # default to all flight types
        multi=True,
        style={"width": "50%"}
    ),
    dcc.Graph(id="flight_graph")
])


@app.callback(
    Output("flight_graph", "figure"),
    [Input("flight_type_dropdown", "value")]
)
def update_graph(selected_types):
    if df_total.empty or df_avg.empty:
        return go.Figure()  # Return an empty figure if no data is available

    # Create a subplot with a secondary y-axis (for percentages)
    fig = make_subplots(specs=[[{"secondary_y": True}]])

    # Loop over each selected flight type and add traces
    for equipage in selected_types:
        # Bar trace for monthly total flights
        fig.add_trace(
            go.Bar(name=f"{equipage} (Total)",
                   x=df_total.index,
                   y=df_total[equipage],
                   offsetgroup=equipage),
            secondary_y=False
        )
        # Dotted line for daily average flights
        fig.add_trace(
            go.Scatter(name=f"{equipage} Avg/Day",
                       x=df_avg.index,
                       y=df_avg[equipage],
                       mode='lines',
                       line=dict(dash='dot')),
            secondary_y=False
        )
        # Calculate and add a percentage line (percentage share of total flights)
        total_flights = df_total.sum(axis=1)
        percentage = (df_total[equipage] / total_flights) * 100
        fig.add_trace(
            go.Scatter(name=f"{equipage} %",
                       x=df_total.index,
                       y=percentage,
                       mode='lines'),
            secondary_y=True
        )

    # Update layout settings and add a range slider
    fig.update_layout(
        title_text="Historical Monthly IFR Movements<br>(Bars = Monthly Total | Dotted Lines = Daily Avg | Solid Lines = % Share)",
        xaxis_title="Time",
    )
    fig.update_yaxes(title_text="<b>Number of Flights</b>", secondary_y=False)
    fig.update_yaxes(title_text="<b>Percentage</b>", secondary_y=True)

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

    return fig


if __name__ == '__main__':
    app.run_server(debug=True)
