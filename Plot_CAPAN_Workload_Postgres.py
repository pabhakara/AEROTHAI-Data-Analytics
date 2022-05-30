import psycopg2
import psycopg2.extras
import datetime
import math
import time
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from matplotlib.ticker import (AutoMinorLocator, MultipleLocator)
from mpl_toolkits.axes_grid1 import host_subplot
import numpy as np

def tic():
    # Homemade version of matlab tic and toc functions
    import time
    global startTime_for_tictoc
    startTime_for_tictoc = time.time()


def toc():
    import time
    if 'startTime_for_tictoc' in globals():
        print("Elapsed time is " + str(time.time() - startTime_for_tictoc) + " seconds.")
    else:
        print("Toc: start time not set")


tic()

conn_postgres = psycopg2.connect(user = "postgres",
                                  password = "password",
                                  host = "127.0.0.1",
                                  port = "5432",
                                  database = "capan")

workload_record = []
traffic_record = []

with conn_postgres:
    cursor_postgres = conn_postgres.cursor(cursor_factory=psycopg2.extras.DictCursor)

    start_time = pd.Timestamp(2019, 12, 25, 22, 0)
    t = start_time
    k = 0
    step = 5
    window_size = 60 # Window size in minutes

    occupancy_count_df = pd.DataFrame()

    while k <= 60 * 24:
        postgres_sql_text = "SELECT \'" + str(t) + "\' as time, s._2sector,'EC' as actor, sum(a.workload) as workload " \
                        "from sector_list s " \
                        "LEFT JOIN" \
                        "(SELECT _3airspace, _6actor, " \
                        " sum(_7weight / 1 / 60) / 60 * 100 as workload " \
                        "FROM public.task_output_2022_05_27_traffic_100 " \
                        "WHERE _6actor like \'EC\' and  " \
                        "event_time >= \'" + str(t + pd.DateOffset(minutes=-window_size/2)) + "\' and " \
                        "event_time < \'" + str(t + pd.DateOffset(minutes=window_size/2)) + "\' and " \
                        "_3airspace like 'SECTOR%' " \
                        "group by _3airspace, _6actor) a " \
                        "ON a._3airspace = s._2sector " \
                        "group by s._2sector, a._6actor " \
                        "order by s._2sector ASC;"
        cursor_postgres.execute(postgres_sql_text)
        workload_record_temp = cursor_postgres.fetchall()
        workload_record += workload_record_temp

        postgres_sql_text = "SELECT \'" + str(t) + "\' as time, s._2sector, sum(a.traffic) as entry_count " \
                            "from sector_list s " \
                            "LEFT JOIN " \
                            "(SELECT _2sector, " \
                            " count(*) as traffic " \
                            "FROM public.sectorcrossing_output_2022_05_27_traffic_100 " \
                            "WHERE _1center like \'VTBB\' and  " \
                            "entry_time >= \'" + str(t + pd.DateOffset(minutes=-window_size/2)) + "\' and " \
                            "entry_time < \'" + str(t + pd.DateOffset(minutes=window_size/2)) + "\' and " \
                            "_2sector like 'SECTOR%' " \
                            "group by _2sector) a " \
                            "ON a._2sector = s._2sector " \
                            "group by s._2sector " \
                            "order by s._2sector;"
        cursor_postgres.execute(postgres_sql_text)
        traffic_record_temp = cursor_postgres.fetchall()
        traffic_record += traffic_record_temp

        postgres_sql_text = "SELECT \'" + str(t) + "\' as time, " \
                            "_2sector, count(*) as entry " \
                            "FROM public.sectorcrossing_output_2022_05_27_traffic_100 " \
                            "WHERE _1center like \'VTBB\' and " \
                            "entry_time > \'2019-12-25 00:00:00\' and entry_time <  \'" + str(t) + "\'and " \
                            "_2sector like \'SECTOR%\' group by _2sector order by _2sector ASC; "
        cursor_postgres.execute(postgres_sql_text)
        entry_count = cursor_postgres.fetchall()
        entry_count_df = pd.DataFrame(entry_count, columns=['timestamp', 'sector', 'entry_count'])

        postgres_sql_text = "SELECT \'" + str(t) + "\' as time, " \
                             "_2sector, count(*) as exit " \
                             "FROM public.sectorcrossing_output_2022_05_27_traffic_100 " \
                             "WHERE _1center like \'VTBB\' and " \
                             "exit_time > \'2019-12-25 00:00:00\' and exit_time <  \'" + str(t) + "\' and " \
                             "_2sector like \'SECTOR%\' group by _2sector order by _2sector ASC; "
        cursor_postgres.execute(postgres_sql_text)
        exit_count = cursor_postgres.fetchall()
        exit_count_df = pd.DataFrame(exit_count, columns=['timestamp', 'sector', 'exit_count'])

        occupancy_count_temp = pd.DataFrame(exit_count_df.iloc[:, 0:2], columns=['timestamp', 'sector'])
        occupancy_count_temp["occupancy"] = entry_count_df.iloc[:, 2] - exit_count_df.iloc[:, 2]
        #print(occupancy_count_temp)

        #occupancy_count_df = occupancy_count_df.append(occupancy_count_temp,ignore_index=True)

        occupancy_count_df = pd.concat([occupancy_count_df, occupancy_count_temp], ignore_index=True)

        t = t + pd.DateOffset(minutes=step)
        k += step

    workload_df = pd.DataFrame(workload_record, columns=['timestamp', 'sector', 'actor','workload'])
    traffic_df = pd.DataFrame(traffic_record, columns=['timestamp', 'sector', 'entry'])
    combined_df = pd.merge(workload_df, occupancy_count_df, how='inner', on=['timestamp','sector'])
    combined_df = pd.merge(combined_df, traffic_df, how='inner', on=['timestamp', 'sector'])

    print(combined_df)


    sector_list = ['SECTOR_1N', 'SECTOR_1S', 'SECTOR_2N', 'SECTOR_2S','SECTOR_3N', 'SECTOR_3S',
                   'SECTOR_4N', 'SECTOR_4S', 'SECTOR_5N', 'SECTOR_5S','SECTOR_6N', 'SECTOR_6S']

    #sector_list = ['SECTOR_3N']

    output_filepath = '/Users/pongabha/Dropbox/Workspace/airspace analysis/FIR Capacity Study 2022/Output Plots/'

    for sector in sector_list:
        # workload_df_temp = workload_df.loc[workload_df['airspace'] == sector]
        # workload_df_temp.plot(x='timestamp',y='workload')

        combined_df_temp = combined_df.loc[traffic_df['sector'] == sector]
        #workload_df_temp = workload_df.loc[workload_df['sector'] == sector]

        fig, ax = plt.subplots()


        ax.plot(combined_df_temp['timestamp'], combined_df_temp['occupancy'], color='k', label='occupancy_count')
        ax.plot(combined_df_temp['timestamp'], combined_df_temp['entry'], color='b', label='entry_count')
        ax.plot(combined_df_temp['timestamp'], combined_df_temp['workload'], color='r', label='workload')

        ax.set_xlabel('Time (UTC)')
        ax.set_ylabel('No. Flights / Workload (%)', color='k')
        plt.xticks(rotation=90)

        ax.xaxis.set_major_locator(MultipleLocator(12))
        ax.yaxis.set_major_locator(MultipleLocator(10))

        # Change minor ticks to show every 5. (20/4 = 5)
        ax.xaxis.set_minor_locator(AutoMinorLocator(1))
        ax.yaxis.set_minor_locator(AutoMinorLocator(1))
        ax.set_ylim([0, 120])

        # ax2 = ax.twinx()
        # ax2.plot(combined_df_temp['timestamp'], combined_df_temp['workload'], color='r', label='workload')
        # ax2.set_ylabel('Workload', color='k')
        # ax2.xaxis.set_major_locator(MultipleLocator(12))
        # #ax2.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M:%S'))
        # ax2.yaxis.set_major_locator(MultipleLocator(10))
        #
        # # Change minor ticks to show every 5. (20/4 = 5)
        # ax2.xaxis.set_minor_locator(AutoMinorLocator(1))
        # #ax2.xaxis.set_minor_formatter(mdates.DateFormatter('%H:%M:%S'))
        # ax2.yaxis.set_minor_locator(AutoMinorLocator(1))
        # ax2.set_ylim([0, 120])

        plt.title( "Scenario Traffic 100% " + "EC Workload: Sector " + sector[-2:])
        plt.xticks(rotation=90)
        plt.legend()
        plt.grid()
        plt.tight_layout()
        plt.savefig(output_filepath + 'traffic_100_EC_'+sector+'.png')

toc()