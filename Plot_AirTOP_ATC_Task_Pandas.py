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

dict_of_df = {}

for filename in filenames:
    airtop_df = pd_read_airtop_csv(input_filepath + filename,dtype_list=None)
    dict_of_df[filename] = airtop_df

#print(dict_of_df.keys())
for filename in filenames:
    print(filename)
    print(dict_of_df[filename])

# sectorcrossing_input_file = "/report/events/changeevents/FLIGHT_ATC_SECTOR_ENTERED.csv"
#
# sectorcrossing_df = pd_read_airtop_csv(root_path + scenario + sectorcrossing_input_file,
#                                        {'Time': 'S',
#                                         'DurationInPreviousATCSector': 'S',
#                                         'OldCurrentATCSector': 'category',
#                                         'NewCurrentATCSector': 'category'}
#                                        )
#
# sectorcrossing_df['Time'] = '2019-12-' + sectorcrossing_df['Time']
#
# datetime_format = "%Y-%m-%d %H:%M:%S"
#
# sectorcrossing_df['Time'] = sectorcrossing_df['Time'].apply(lambda x:datetime.strptime(x,datetime_format))
#
# # Sector Skip Filter
# filter = (sectorcrossing_df['DurationInPreviousATCSector'] > '00:02:00')
#
# sectorcrossing_df = sectorcrossing_df.loc[filter]
#
# print(sectorcrossing_df)
#
# sector_entry_df = sectorcrossing_df[sectorcrossing_df['NewCurrentATCSector'].isnull() == 0]
# sector_entry_df = sector_entry_df[sector_entry_df['NewCurrentATCSector'].str.contains('SECTOR_')]
#
# sector_exit_df = sectorcrossing_df[sectorcrossing_df['OldCurrentATCSector'].isnull() == 0]
# sector_exit_df = sector_exit_df[sector_exit_df['OldCurrentATCSector'].str.contains('SECTOR_')]
#
# column_selection = ['Time', 'NewCurrentATCSector', ]
# sector_entry_df = sector_entry_df[column_selection].copy()
# sector_entry_df.rename(columns={'NewCurrentATCSector': 'Sector'}, inplace=True)
#
# column_selection = ['Time', 'OldCurrentATCSector', ]
# sector_exit_df = sector_exit_df[column_selection].copy()
# sector_exit_df.rename(columns={'OldCurrentATCSector': 'Sector'}, inplace=True)
