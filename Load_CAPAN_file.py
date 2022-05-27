import pandas as pd

root_path = "/Users/pongabha/Dropbox/Workspace/airspace analysis/FIR Capacity Study 2022/"
scenario = "BANGKOK_ACC - 2022-05-20 - Traffic 100%/"
filepath = "RUNS/12SEC_VTBS19_NO_MIL/output/sectorcrossing.out.1"

sectorcrossing_df = pd.read_csv(root_path + scenario + filepath ,delimiter=';',header=0)
sectorcrossing_df.columns = sectorcrossing_df.columns.str.replace(' ','')

filepath = "RUNS/12SEC_VTBS19_NO_MIL/output/task.out.1"

task_df = pd.read_csv(root_path + scenario + filepath ,delimiter=';',header=0)
task_df.columns = task_df.columns.str.replace(' ','')

print(task_df)

print(task_df.columns)

print(task_df[task_df['3Airspace'] == 'SECTOR_1N'])