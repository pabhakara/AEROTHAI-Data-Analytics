import pandas as pd
from datetime import datetime

import numpy as np
import matplotlib.pyplot as plt
from matplotlib.ticker import (AutoMinorLocator, MultipleLocator)
from matplotlib.dates import DateFormatter

from os import walk

def tic():
    #Homemade version of matlab tic and toc functions
    import time
    global startTime_for_tictoc
    startTime_for_tictoc = time.time()


def toc():
    import time
    if 'startTime_for_tictoc' in globals():
        print("Elapsed time is " + str(time.time() - startTime_for_tictoc) + " seconds.")
    else:
        print("Toc: start time not set")

def pd_read_airtop_csv(airtop_csv_file,dtype_list=None):
    column_names = open(airtop_csv_file).readlines()[1].split(' ')
    print(column_names)
    airtop_df = pd.read_csv(airtop_csv_file,
                                    delimiter=';', header=3,
                                    names=column_names,
                                    dtype=dtype_list)
    return airtop_df

tic()

traffic_percentage = '100'

root_path = "/Users/pongabha/Library/CloudStorage/Dropbox/Workspace/airspace analysis/AirTOP Workspace/Projects"
scenario = f"/BKK_FIR_AIRAC-2024-13_BKK_RWY_19-20-21"

input_filepath = f"{root_path}{scenario}" + "/report/events/changeevents/"

output_filepath = f"{root_path}{scenario}"

filenames = next(walk(input_filepath), (None, None, []))[2]  # [] if no file
#
# print(filenames)

filenames = ['FLIGHT_ATC_SECTOR_ENTERED.csv']

for filename in filenames:
    print(input_filepath + filename)
    airtop_pd = pd_read_airtop_csv(input_filepath + filename,dtype_list=None)
    print(airtop_pd)

sectorcrossing_input_file = "/report/events/changeevents/FLIGHT_ATC_SECTOR_ENTERED.csv"
#
# column_names = open(root_path + scenario + sectorcrossing_input_file).readlines()[1].split(' ')
#
# sectorcrossing_df = pd.read_csv(root_path + scenario + sectorcrossing_input_file,
#                                 delimiter=';', header=3,
#                                 names = column_names,
#                                 dtype={'Time': 'S',
#                                        'DurationInPreviousATCSector': 'S',
#                                        'OldCurrentATCSector': 'category',
#                                        'NewCurrentATCSector': 'category'})
#
sectorcrossing_df = pd_read_airtop_csv(root_path + scenario + sectorcrossing_input_file,
                                       {'Time': 'S',
                                        'DurationInPreviousATCSector': 'S',
                                        'OldCurrentATCSector': 'category',
                                        'NewCurrentATCSector': 'category'}
                                       )

sectorcrossing_df['Time'] = '2019-12-' + sectorcrossing_df['Time']

datetime_format = "%Y-%m-%d %H:%M:%S"

sectorcrossing_df['Time'] = sectorcrossing_df['Time'].apply(lambda x:datetime.strptime(x,datetime_format))

# Sector Skip Filter
filter = (sectorcrossing_df['DurationInPreviousATCSector'] > '00:02:00')

sectorcrossing_df = sectorcrossing_df.loc[filter]

print(sectorcrossing_df['DurationInPreviousATCSector'])

sector_entry_df = sectorcrossing_df[sectorcrossing_df['NewCurrentATCSector'].isnull() == 0]
sector_entry_df = sector_entry_df[sector_entry_df['NewCurrentATCSector'].str.contains('SECTOR_')]

sector_exit_df = sectorcrossing_df[sectorcrossing_df['OldCurrentATCSector'].isnull() == 0]
sector_exit_df = sector_exit_df[sector_exit_df['OldCurrentATCSector'].str.contains('SECTOR_')]

column_selection = ['Time', 'NewCurrentATCSector', ]
sector_entry_df = sector_entry_df[column_selection].copy()
sector_entry_df.rename(columns={'NewCurrentATCSector': 'Sector'}, inplace=True)

column_selection = ['Time', 'OldCurrentATCSector', ]
sector_exit_df = sector_exit_df[column_selection].copy()
sector_exit_df.rename(columns={'OldCurrentATCSector': 'Sector'}, inplace=True)

#--------------

workload_input_file = "/report/events/watchevents/AIRSPACE_WORKLOAD.csv"

column_names = open(root_path + scenario + workload_input_file).readlines()[1].split(' ')

workload_df = pd.read_csv(root_path + scenario + workload_input_file, delimiter=';', header=3,
                          names = column_names,
                          dtype={'Time': 'S',
                             'Workload': 'I',
                             'Airspace': 'category'}, )

column_selection = ['Time', 'Airspace', 'Workload', ]

workload_df['Time'] = '2019-12-' + workload_df['Time']

workload_df['Time'] = workload_df['Time'].apply(lambda x:datetime.strptime(x,datetime_format))

workload_df = workload_df[column_selection].copy()

workload_df = workload_df[workload_df['Airspace'].str.contains('SECTOR_')]

workload_df.rename(columns={'Airspace': 'Sector', 'Time': 'TimeStamp'}, inplace=True)

start_time = pd.Timestamp(2019, 12, 25, 0, 0, 0, 0)

t = start_time

k = 0
step = 5
window_size = 60  # Window size in minutes
#
# workload_df = pd.DataFrame()
occupancy_count_df = pd.DataFrame()
traffic_count_df = pd.DataFrame()
exit_count_df = pd.DataFrame()
#
sector_list = ['SECTOR_1N', 'SECTOR_1S', 'SECTOR_2N', 'SECTOR_2S', 'SECTOR_3N', 'SECTOR_3S',
               'SECTOR_4N', 'SECTOR_4S', 'SECTOR_5N', 'SECTOR_5S', 'SECTOR_6N', 'SECTOR_6S',]
#
sector_list_df = pd.DataFrame(sector_list)
#
# Total sector entry counts in 24 hours
filter = (sector_entry_df['Time'] > str(t + pd.DateOffset(minutes=0))) & \
         (sector_entry_df['Time'] < str(t + pd.DateOffset(minutes=24 * 60))) & \
         (sector_entry_df['Sector'].str.contains("SECTOR_"))

temp_df = sector_entry_df.loc[filter, ['Sector', 'Time']]
#
total_sector_crossing_df = temp_df.groupby(['Sector'],observed=False)['Sector'].count()
#
#print(total_sector_crossing_df)

while k <= 60 * 24:
    # ------------- traffic entry count --------------------

    filter = (sector_entry_df['Time'] > str(t + pd.DateOffset(minutes=-window_size / 2))) & \
             (sector_entry_df['Time'] < str(t + pd.DateOffset(minutes=window_size / 2))) & \
             (sector_entry_df['Sector'].str.contains("SECTOR_"))

    temp_df = sector_entry_df.loc[filter, ['Sector', 'Time']]

    z = temp_df.groupby(['Sector']).count()
    z = z.reset_index()
    z = z[z['Sector'].str.contains('SECTOR_')]

    z['TimeStamp'] = t
    z.rename(columns={'Time': 'Entry_Count'}, inplace=True)

    traffic_count_df = pd.concat([traffic_count_df, z], ignore_index=True)

    # --------------- occupancy count ------------

    # filter = (sector_exit_df['Time'] > str(t + pd.DateOffset(minutes=-window_size / 2))) & \
    #          (sector_exit_df['Time'] < str(t + pd.DateOffset(minutes=window_size / 2))) & \
    #          (sector_exit_df['Sector'].str.contains("SECTOR_"))
    #
    # temp_df = sector_exit_df.loc[filter, ['Sector', 'Time']]
    #
    # z = temp_df.groupby(['Sector']).count()
    # z = z.reset_index()
    # z = z[z['Sector'].str.contains('SECTOR_')]
    # z['TimeStamp'] = t.strftime("%Y-%m-%d %H:%M:%S")
    # z.rename(columns={'Time': 'Exit_Count'}, inplace=True)
    #
    # exit_count_df = pd.concat([exit_count_df, z], ignore_index=True)
    # print(traffic_count_df)
    # print(exit_count_df)
    #
    # y = pd.DataFrame((traffic_count_df.iloc[:] - exit_count_df.iloc[:]))
    #
    # print(y)

    # y['Timestamp'] = t.strftime("%Y-%m-%d %H:%M")
    # y.rename(columns={'Sector': 'Occupancy_Count'}, inplace=True)
    # y = y.reset_index()
    # y = y[y['Sector'].str.contains('SECTOR_')]
    #
    # occupancy_count_df = pd.concat([occupancy_count_df, y])
    # occupancy_count_df = occupancy_count_df[occupancy_count_df['2Sector'].str.contains('SECTOR_')]
    #
    t = t + pd.DateOffset(minutes=step)
    k += step

#print(traffic_count_df)

# sector_list = ['SECTOR_1N', 'SECTOR_1S', 'SECTOR_2N', 'SECTOR_2S', 'SECTOR_3N', 'SECTOR_3S',
#                     'SECTOR_4N', 'SECTOR_4S', 'SECTOR_5N', 'SECTOR_5S', 'SECTOR_6N', 'SECTOR_6S',
#                     'SECTOR_7N', 'SECTOR_7S', 'SECTOR_8N', 'SECTOR_8S']
# sector_list = ['SECTOR_1N', 'SECTOR_1S', 'SECTOR_2N', 'SECTOR_2S', 'SECTOR_3N', 'SECTOR_3S',
# 'SECTOR_4N', 'SECTOR_4S', 'SECTOR_5N', 'SECTOR_5S', 'SECTOR_6N', 'SECTOR_6S']
#
# occupancy_count_df = occupancy_count_df[occupancy_count_df['2Sector'].str.contains('SECTOR_')]
#
# workload_df.rename(columns={'Airspace': 'Sector'}, inplace=True)
# occupancy_count_df.rename(columns={'2Sector': 'Sector'}, inplace=True)
# traffic_count_df.rename(columns={'Airspace': 'Sector'}, inplace=True)
#
# combined_df = pd.merge(workload_df, occupancy_count_df, how='inner', on=['Timestamp', 'Sector'])
# combined_df = pd.merge(combined_df, traffic_count_df, how='inner', on=['Time', 'Sector'])

workload_df['TimeStamp'] = pd.to_datetime(workload_df['TimeStamp'])
traffic_count_df['TimeStamp'] = pd.to_datetime(traffic_count_df['TimeStamp'])

combined_df = pd.merge(workload_df, traffic_count_df, how='inner', on=['TimeStamp', 'Sector'])

combined_df = combined_df.set_index('TimeStamp')

print(combined_df)


for sector in sector_list:

    combined_df_temp = combined_df.loc[combined_df['Sector'] == sector]

    fig, ax = plt.subplots()

    ax.plot(combined_df_temp.index, combined_df_temp['Workload'], color='r', label='Workload')
    #ax.plot(combined_df_temp.index, combined_df_temp['Occupancy_Count'], color='k', label='Occupancy Count')
    ax.plot(combined_df_temp.index, combined_df_temp['Entry_Count'], color='b', label='Entry Count')

    #
    hh_mm = DateFormatter("%H:%M")
    ax.xaxis.set_major_formatter(hh_mm)


    ax.hlines(y=70, xmin=combined_df_temp.index[0], xmax=combined_df_temp.index[-1], color='g')
#
    ax.set_xlabel('Time (UTC)')
    ax.set_ylabel('No. Flights / Workload (%)', color='k')

    plt.xticks(rotation=90)

    # ax.xaxis.set_major_locator(MultipleLocator(12))
    # ax.yaxis.set_major_locator(MultipleLocator(10))

    #ax.xaxis.set_minor_locator(AutoMinorLocator(1))
    ax.yaxis.set_minor_locator(AutoMinorLocator(1))

    ax.set_ylim([0, 100])
    ax.set_xlim([combined_df_temp.index[0], combined_df_temp.index[-1]])

    num_flights = total_sector_crossing_df.loc[sector]

    plt.title(f"Scenario Traffic {traffic_percentage}% ({num_flights}" + \
              f" flights) EC Workload: Sector {sector[-2:]})")
    plt.xticks(rotation=90)
    plt.legend()
    plt.grid()
    plt.tight_layout()
    plt.savefig(output_filepath + "/Workload-traffic_" + traffic_percentage + "_EC_" + sector + ".png")

    # ----------------

    fig, ax = plt.subplots()
    ax.plot(combined_df_temp['Entry_Count'],combined_df_temp['Workload'] , 'k.')
    ax.xaxis.set_major_locator(MultipleLocator(12))
    ax.yaxis.set_major_locator(MultipleLocator(10))
    ax.xaxis.set_minor_locator(AutoMinorLocator(1))
    ax.yaxis.set_minor_locator(AutoMinorLocator(1))
    ax.set_ylim([0, 120])
    ax.set_xlabel('No. Flights', color='k')
    ax.set_ylabel('Workload (%)')


    plt.title("Scenario Traffic " + traffic_percentage + "% EC Workload: Sector " + sector[-2:])
    plt.xticks(rotation=90)
    plt.legend()
    plt.grid()
    plt.tight_layout()
    plt.savefig(output_filepath + "/Capacity-traffic_" + traffic_percentage + "_EC_" + sector + ".png")

toc()
