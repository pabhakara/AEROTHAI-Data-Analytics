import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from matplotlib.ticker import (AutoMinorLocator, MultipleLocator)
from matplotlib.dates import DateFormatter

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

tic()

traffic_percentage = '130'

root_path = "/Users/pongabha/Library/CloudStorage/Dropbox/Workspace/airspace analysis/FIR Capacity Study"


# scenario = f"/2023/FIR_Capacity_2023-09-26 - Traffic {traffic_percentage}%"

scenario = f"/2023/FIR_Capacity_2023-09-26 - 12 Sector Traffic {traffic_percentage}%"

# scenario = f"/BKK_FIR_baseline_12Sectors_BKK_APP_Sectors_01-02-21_extended"

# scenario = f"/New_Northern_Routes 2023-09-06"

output_filepath = f"/Users/pongabha/Library/CloudStorage/Dropbox/Workspace/airspace analysis/" \
                  f"FIR Capacity Study/2023/Output Plots/"

# output_filepath = f"/Users/pongabha/Library/CloudStorage/Dropbox/Workspace/airspace analysis/" \
#                   f"FIR Capacity Study{scenario}/Output Plots/"

sectorcrossing_input_file = "/report/sectorcrossing.out.1"

sectorcrossing_df = pd.read_csv(root_path + scenario + sectorcrossing_input_file,
                                delimiter=';',header=0,
                                dtype={' 13ActualEntryHH': 'S',
                                       ' 14ActualExitHH': 'S',
                                       ' 2Sector': 'category'},)
sectorcrossing_df.columns = sectorcrossing_df.columns.str.replace(' ','')
sectorcrossing_df.columns = sectorcrossing_df.columns.str.replace('{','',regex=True)
sectorcrossing_df = sectorcrossing_df.iloc[: , :-1]
sectorcrossing_df = sectorcrossing_df[sectorcrossing_df['2Sector'].str.contains('SECTOR_')]
sectorcrossing_df = sectorcrossing_df[sectorcrossing_df['28SectorEvent'].str.contains('#SX')]
sectorcrossing_df = sectorcrossing_df[sectorcrossing_df['12SchTimeMinutes'] > 2]
column_selection = ['1Center', '2Sector','13ActualEntryHH','14ActualExitHH']
sectorcrossing_df = sectorcrossing_df[column_selection].copy()

task_input_file = "/output/task.out.1"

task_df = pd.read_csv(root_path + scenario + task_input_file ,delimiter=';',header=0,
                                dtype={' 6Actor': 'category',
                                       ' 4Category': 'category',
                                       ' 5CategoryActivity': 'category',
                                       ' 8TaskName': 'category',
                                       ' 3Airspace': 'category'},)

task_df.columns = task_df.columns.str.replace(' ','')
task_df.columns = task_df.columns.str.replace('{','',regex=True)
task_df = task_df.iloc[: , :-1]
task_df = task_df[task_df['3Airspace'].str.contains('SECTOR_')]
column_selection = ['1Time', '3Airspace', '4Category','5CategoryActivity','6Actor','7Weight','8TaskName']
task_df = task_df[column_selection].copy()

ref_time = pd.Timestamp(2019, 12, 25, 0, 0)
start_time = pd.Timestamp(2019, 12, 25, 22, 0)

task_df['task_time'] = task_df['1Time'].apply(pd.to_timedelta) + ref_time
task_df.drop(columns=['1Time'],inplace=True)


column_selection = ['1Center', '2Sector','13ActualEntryHH','14ActualExitHH']
sectorcrossing_df = sectorcrossing_df[column_selection].copy()

sectorcrossing_df['13ActualEntryHH'] = sectorcrossing_df['13ActualEntryHH'].str[:2] + ':' + \
                                       sectorcrossing_df['13ActualEntryHH'].str[2:4] + ':' + \
                                       sectorcrossing_df['13ActualEntryHH'].str[-2:]

sectorcrossing_df['entry_time'] = sectorcrossing_df['13ActualEntryHH'].apply(pd.to_timedelta) + ref_time

sectorcrossing_df['14ActualExitHH'] = sectorcrossing_df['14ActualExitHH'].str[:2] + ':' + \
                                       sectorcrossing_df['14ActualExitHH'].str[2:4] + ':' + \
                                       sectorcrossing_df['14ActualExitHH'].str[-2:]

sectorcrossing_df['exit_time'] = sectorcrossing_df['14ActualExitHH'].apply(pd.to_timedelta) + ref_time

t = start_time

k = 0
step = 5
window_size = 60  # Window size in minutes

workload_df = pd.DataFrame()
occupancy_count_df = pd.DataFrame()
traffic_count_df = pd.DataFrame()

sector_list = ['SECTOR_1N', 'SECTOR_1S', 'SECTOR_2N', 'SECTOR_2S', 'SECTOR_3N', 'SECTOR_3S',
                    'SECTOR_4N', 'SECTOR_4S', 'SECTOR_5N', 'SECTOR_5S', 'SECTOR_6N', 'SECTOR_6S',
                    'SECTOR_7N', 'SECTOR_7S', 'SECTOR_8N', 'SECTOR_8S']

sector_list = [
    'APE1', 'APE2', 'APN', 'APS1', 'APS2',
    'APW', 'ARR19_1', 'ARR19_2', 'DAR21',
    'DDP21', 'DSB19'
]

sector_list_df = pd.DataFrame(sector_list)

# Total sector entry counts in 24 hours
filter = (sectorcrossing_df['entry_time'] > str(t + pd.DateOffset(minutes=0))) & \
         (sectorcrossing_df['entry_time'] < str(t + pd.DateOffset(minutes=24*60))) & \
         (sectorcrossing_df['2Sector'].str.contains("SECTOR_")) & \
         (sectorcrossing_df['1Center'] == "VTBB")

temp_df = sectorcrossing_df.loc[filter, ['2Sector', 'entry_time']]

total_sector_crossing_df = temp_df.groupby(['2Sector'])['2Sector'].count()

while k <= 60 * 24:
    # ------- workload count ------------
    filter = (task_df['task_time'] > str(t + pd.DateOffset(minutes=-window_size/2))) & \
            (task_df['task_time'] < str(t + pd.DateOffset(minutes=window_size/2))) & \
            (task_df['6Actor'] == 'EC') & \
            (task_df['3Airspace'].str.contains("SECTOR_"))
    temp_df = task_df[filter]

    x = temp_df.groupby(['3Airspace']).sum()/36
    x = x.reset_index()

    x['Timestamp'] = t.strftime("%Y-%m-%d %H:%M")

    workload_df = pd.concat([workload_df, x], ignore_index=True)

    # ------------- traffic entry count --------------------

    filter = (sectorcrossing_df['entry_time'] > str(t + pd.DateOffset(minutes=-window_size / 2))) & \
             (sectorcrossing_df['entry_time'] < str(t + pd.DateOffset(minutes=window_size / 2))) & \
             (sectorcrossing_df['2Sector'].str.contains("SECTOR_")) & \
             (sectorcrossing_df['1Center'] == "VTBB")

    temp_df = sectorcrossing_df.loc[filter,['2Sector','entry_time']]
    z = temp_df.groupby(['2Sector']).count()
    z = z.reset_index()
    z = z[z['2Sector'].str.contains('SECTOR_')]
    z['Timestamp'] = t.strftime("%Y-%m-%d %H:%M")
    z.rename(columns={'entry_time': 'Entry_Count'}, inplace=True)
    traffic_count_df = pd.concat([traffic_count_df, z], ignore_index=True)

    # --------------- occupancy count ------------

    filter_exit = (sectorcrossing_df['exit_time'] > str(ref_time)) & \
             (sectorcrossing_df['exit_time'] < str(t)) & \
             (sectorcrossing_df['2Sector'].str.contains("SECTOR_")) & \
             (sectorcrossing_df['1Center'] == "VTBB")

    exit_df = sectorcrossing_df[filter_exit]
    exit_count_df = exit_df.groupby(['2Sector'])['2Sector'].count()

    filter_entry = (sectorcrossing_df['entry_time'] > str(ref_time)) & \
             (sectorcrossing_df['entry_time'] < str(t)) & \
             (sectorcrossing_df['2Sector'].str.contains("SECTOR_")) & \
             (sectorcrossing_df['1Center'] == "VTBB")

    entry_df = sectorcrossing_df[filter_entry]
    entry_count_df = entry_df.groupby(['2Sector'])['2Sector'].count()

    y = pd.DataFrame((entry_count_df.iloc[:] - exit_count_df.iloc[:]))
    y['Timestamp'] = t.strftime("%Y-%m-%d %H:%M")
    y.rename(columns={'2Sector': 'Occupancy_Count'}, inplace=True)
    y = y.reset_index()
    y = y[y['2Sector'].str.contains('SECTOR_')]

    occupancy_count_df = pd.concat([occupancy_count_df, y])
    occupancy_count_df = occupancy_count_df[occupancy_count_df['2Sector'].str.contains('SECTOR_')]

    t = t + pd.DateOffset(minutes=step)
    k += step

sector_list = ['SECTOR_1N', 'SECTOR_1S', 'SECTOR_2N', 'SECTOR_2S', 'SECTOR_3N', 'SECTOR_3S',
                    'SECTOR_4N', 'SECTOR_4S', 'SECTOR_5N', 'SECTOR_5S', 'SECTOR_6N', 'SECTOR_6S',
                    'SECTOR_7N', 'SECTOR_7S', 'SECTOR_8N', 'SECTOR_8S']
sector_list = ['SECTOR_1N', 'SECTOR_1S', 'SECTOR_2N', 'SECTOR_2S', 'SECTOR_3N', 'SECTOR_3S',
'SECTOR_4N', 'SECTOR_4S', 'SECTOR_5N', 'SECTOR_5S', 'SECTOR_6N', 'SECTOR_6S']

occupancy_count_df = occupancy_count_df[occupancy_count_df['2Sector'].str.contains('SECTOR_')]

workload_df.rename(columns={'3Airspace': 'Sector'}, inplace=True)
occupancy_count_df.rename(columns={'2Sector': 'Sector'}, inplace=True)
traffic_count_df.rename(columns={'2Sector': 'Sector'}, inplace=True)

combined_df = pd.merge(workload_df, occupancy_count_df, how='inner', on=['Timestamp', 'Sector'])
combined_df = pd.merge(combined_df, traffic_count_df, how='inner', on=['Timestamp', 'Sector'])
combined_df = combined_df.set_index('Timestamp')

#print(combined_df)

for sector in sector_list:

    combined_df_temp = combined_df.loc[combined_df['Sector'] == sector]

    fig, ax = plt.subplots()

    ax.plot(combined_df_temp.index, combined_df_temp['7Weight'], color='r', label='Workload')
    ax.plot(combined_df_temp.index, combined_df_temp['Occupancy_Count'], color='k', label='Occupancy Count')
    ax.plot(combined_df_temp.index, combined_df_temp['Entry_Count'], color='b', label='Entry Count')

    # hh_mm = DateFormatter('%D')
    # ax.xaxis.set_major_formatter(hh_mm)

    print(sector)

    ax.hlines(y=70, xmin=combined_df_temp.index[0], xmax=combined_df_temp.index[-1], color='g')

    ax.set_xlabel('Time (UTC)')
    ax.set_ylabel('No. Flights / Workload (%)', color='k')

    plt.xticks(rotation=90)

    ax.xaxis.set_major_locator(MultipleLocator(12))
    ax.yaxis.set_major_locator(MultipleLocator(10))

    ax.xaxis.set_minor_locator(AutoMinorLocator(1))
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
    plt.savefig(output_filepath + "Workload-traffic_" + traffic_percentage + "_EC_" + sector + ".png")

    # ----------------

    # fig, ax = plt.subplots()
    # ax.plot(combined_df_temp['Entry_Count'],combined_df_temp['7Weight'] , 'k.')
    # ax.xaxis.set_major_locator(MultipleLocator(12))
    # ax.yaxis.set_major_locator(MultipleLocator(10))
    # ax.xaxis.set_minor_locator(AutoMinorLocator(1))
    # ax.yaxis.set_minor_locator(AutoMinorLocator(1))
    # ax.set_ylim([0, 120])
    # ax.set_xlabel('No. Flights', color='k')
    # ax.set_ylabel('Workload (%)')
    #
    #
    # plt.title("Scenario Traffic " + traffic_percentage + "% EC Workload: Sector " + sector[-2:])
    # #plt.xticks(rotation=90)
    # #plt.legend()
    # plt.grid()
    # plt.tight_layout()
    # plt.savefig(output_filepath + "Capacity-traffic_" + traffic_percentage + "_EC_" + sector + ".png")

toc()