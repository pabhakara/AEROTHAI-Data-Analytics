import psycopg2
import psycopg2.extras
import datetime
import math
import time
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.ticker import (AutoMinorLocator, MultipleLocator)
import numpy as np

t = time.time()

conn_postgres = psycopg2.connect(user = "postgres",
                                  password = "password",
                                  host = "127.0.0.1",
                                  port = "5432",
                                  database = "capan")

record = []

with conn_postgres:
    cursor_postgres = conn_postgres.cursor(cursor_factory=psycopg2.extras.DictCursor)

    start_time = pd.Timestamp(2019, 12, 25, 22, 0)
    t = start_time
    k = 0
    step = 5
    window_size = 60 # Window size in minutes
    while k <= 60 * 24:
        postgres_sql_text = "SELECT \'" + str(t) + "\' as time, " \
                        "_3airspace, _6actor, " \
                        " sum(_7weight / 1 / 60) / 60 * 100 as workload " \
                        "FROM public.task_output_2022_05_18_traffic_100 " \
                        "WHERE _6actor like \'EC\' and  " \
                        "event_time > \'" + str(t + pd.DateOffset(minutes=-window_size/2)) + "\' and " \
                        "event_time < \'" + str(t + pd.DateOffset(minutes=window_size/2)) + "\' and " \
                        "_3airspace like 'SECTOR%' " \
                        "group by _3airspace, _6actor "
        cursor_postgres.execute(postgres_sql_text)
        record_temp = cursor_postgres.fetchall()
        record += record_temp
        t = t + pd.DateOffset(minutes=step)
        k += step

    df = pd.DataFrame(record, columns=['timestamp', 'airspace', 'actor','workload'])

    sector_list = ['SECTOR_1N', 'SECTOR_2N', 'SECTOR_3N', 'SECTOR_4N','SECTOR_5N', 'SECTOR_6N',
                   'SECTOR_1S', 'SECTOR_2S', 'SECTOR_3S', 'SECTOR_4S','SECTOR_5S', 'SECTOR_6S']

    output_filepath = '/Users/pongabha/Dropbox/Workspace/airspace analysis/FIR Capacity Study 2022/Output Plots/'

    for sector in sector_list:
        df_temp = df.loc[df['airspace'] == sector]
        df_temp.plot(x='timestamp',y='workload')
        plt.plot()
        plt.title(label = sector)
        plt.xticks(rotation=90)
        ax = plt.gca()
        ax.set_ylim([0, 120])
        # Set axis ranges; by default this will put major ticks every 25.
       #  ax.set_xlim(0, 200)

        # Change major ticks to show every 20.
        ax.xaxis.set_major_locator(MultipleLocator(10))
        ax.yaxis.set_major_locator(MultipleLocator(10))

        # Change minor ticks to show every 5. (20/4 = 5)
        ax.xaxis.set_minor_locator(AutoMinorLocator(1))
        ax.yaxis.set_minor_locator(AutoMinorLocator(1))

        plt.grid()
        plt.tight_layout()
        plt.savefig(output_filepath + 'traffic_100_EC_'+sector+'.png')
