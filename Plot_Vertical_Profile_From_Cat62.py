import psycopg2
import psycopg2.extras
import pandas as pd
import plotly.express as px
import matplotlib.pyplot as plt
from matplotlib.ticker import (AutoMinorLocator, MultipleLocator)
from matplotlib.dates import DateFormatter
import os
from dash import Dash, html, dcc
import time
import datetime as dt
def none_to_null(etd):
    if etd == 'None':
        x = 'null'
    else:
        x = "'" + etd + "'"
    return x

conn_postgres_source = psycopg2.connect(user="pongabhaab",
                                             password="pongabhaab",
                                             host="172.16.129.241",
                                             port="5432",
                                             database="aerothai_dwh",
                                             options="-c search_path=dbo,sur_air")

# conn_postgres_source = psycopg2.connect(user="postgres",
#                                              password="password",
#                                              host="localhost",
#                                              port="5432",
#                                              database="temp",
#                                              options="-c search_path=dbo,sur_air")

output_filepath = '/Users/pongabha/Dropbox/Workspace/AEROTHAI Data Analytics/Flight_Proflie_Plots/'

year = '2022'
month = '07'
day = '01'

with conn_postgres_source:
    cursor_postgres_source = conn_postgres_source.cursor(cursor_factory=psycopg2.extras.DictCursor)
    # Create an SQL query that selects the list of flights that we want plot
    # from the source PostgreSQL database
    postgres_sql_text = f"SELECT DISTINCT t.flight_key " \
                        f"FROM sur_air.cat062_{year}{month}{day} t " \
                        f"LEFT JOIN flight_data.flight_{year}{month} f " \
                        f"ON t.flight_id = f.id " \
                        f"WHERE (f.dep LIKE 'VTSP' AND f.dest LIKE 'VTBS') " \
                        f"AND t.acid LIKE '%' " \
                        f"AND f.frule LIKE 'I'; "
    cursor_postgres_source.execute(postgres_sql_text)
    flight_key_list = cursor_postgres_source.fetchall()
    print(flight_key_list)

with conn_postgres_source:
    for flight_key in flight_key_list:
        print(flight_key[0])
        cursor_postgres_source = conn_postgres_source.cursor(cursor_factory=psycopg2.extras.DictCursor)
        # Create an SQL query that selects surveillance targets based on the list of selected flights
        # from the source PostgreSQL database
        postgres_sql_text = f"SELECT t.flight_key, t.app_time, t.sector, t.dist_from_last_position, t.measured_fl " \
                            f"FROM sur_air.cat062_{year}{month}{day} t " \
                            f"LEFT JOIN flight_data.flight_{year}{month} f " \
                            f"ON t.flight_id = f.id " \
                            f"LEFT JOIN airac_current.airports a " \
                            f"ON a.airport_identifier = f.dest " \
                            f"WHERE f.flight_key = '{flight_key[0]}' " \
                            f"ORDER BY t.app_time "
                            # f"AND public.ST_WITHIN(t.\"2d_position\",public.ST_Buffer(a.geom,2)) " \

        print(postgres_sql_text)
        cursor_postgres_source.execute(postgres_sql_text)
        record = cursor_postgres_source.fetchall()
        df = pd.DataFrame(record, columns=['flight_key','app_time','sector','dist_from_last_position','measured_fl'])
        print(df)

        fig, ax = plt.subplots()

        ax.plot(df['app_time'], df['measured_fl'], color='b')
        # ax.plot(df['app_time'], df['entry'], color='b', label='entry_count')
        # ax.plot(df['app_time'], df['workload'], color='r', label='workload')

        ax.set_xlabel('Time (UTC)')
        #ax.set_ylabel('Measured FL', color='k')
        plt.xticks(rotation=90)

        date_form = DateFormatter("%Y-%m-%d %H:%M")
        ax.xaxis.set_major_formatter(date_form)

        ax.xaxis.set_major_locator(MultipleLocator(1/60/12))
        ax.yaxis.set_major_locator(MultipleLocator(20))

        # Change minor ticks to show every 5. (20/4 = 5)
        ax.xaxis.set_minor_locator(AutoMinorLocator(1/60/12))
        ax.yaxis.set_minor_locator(AutoMinorLocator(1))
        ax.set_ylim([0,460])

        plt.title(f"Flight_Key {flight_key[0]}")
        plt.xticks(rotation=90)
        #plt.legend()
        plt.grid()
        plt.tight_layout()
        plt.savefig(f"{output_filepath}{flight_key[0]}.png")

