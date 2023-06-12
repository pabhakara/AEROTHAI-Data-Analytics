import psycopg2.extras
import pandas as pd
import plotly.graph_objects as go
import datetime as dt
import dash
from dash import dcc
from dash import html
from dash.dependencies import Input, Output
import plotly
import plotly.express as px

def count_dly(code1, code2):
    postgres_sql_text = f"SELECT adep_icao, month, operator,dly_code,COUNT(minutes), " \
        f"SUM(minutes),SUM(minutes)/COUNT(minutes) as dly_per_flight " \
        f"FROM " \
        f"(SELECT adep_icao, " \
        f"extract(month from aobt_utc) as month, " \
        f"left(fltid,3) as operator, " \
        f"'{code1}' as dly_code, (dep_dly->'{code1}')::float as minutes " \
        f"FROM airline_data.flight_data_2022 " \
        f"WHERE NOT dep_dly->'{code1}' IS NULL " \
        f"AND adep_icao LIKE 'VT%' " \
        f"UNION " \
        f"SELECT adep_icao,  " \
        f"extract(month from aobt_utc) as month, " \
        f"left(fltid,3) as operator, " \
        f"'{code2}' as dly_code, (dep_dly->'{code2}')::float as minutes " \
        f"FROM airline_data.flight_data_2022 " \
        f"WHERE NOT dep_dly->'{code2}' IS NULL " \
        f"AND adep_icao LIKE 'VT%' " \
        f") a " \
        f"GROUP BY adep_icao, month, operator, dly_code " \
        f"ORDER BY adep_icao ASC,month ASC,count DESC "
    cursor_postgres = conn_postgres.cursor()
    cursor_postgres.execute(postgres_sql_text)
    record = cursor_postgres.fetchall()
    equipage_count_temp = pd.DataFrame(record)
    return equipage_count_temp


#pd.options.plotting.backend = "plotly"

schema_name = 'airline_data'
# conn_postgres = psycopg2.connect(user="pongabhaab",
#                                  password="pongabhaab",
#                                  host="172.16.129.241",
#                                  port="5432",
#                                  database="aerothai_dwh",
#                                  options="-c search_path=dbo," + schema_name)

# Try to connect to the local PostGresSQL database in which we will store our flight trajectories coupled with FPL data.
conn_postgres = psycopg2.connect(user = "postgres",
                                  password = "password",
                                  host = "127.0.0.1",
                                  port = "5432",
                                  database = "temp")

#date_list = pd.date_range(start='2022-05-01', end='2023-05-20', freq='D')

code1_list = ['0',
'1',
'2',
'3',
'4',
'5',
'6',
'7',
'8',
'9',
'11',
'12',
'13',
'14',
'15',
'16',
'17',
'18',
'19',
'21',
'22',
'23',
'24',
'25',
'26',
'27',
'28',
'29',
'31',
'32',
'33',
'34',
'35',
'36',
'37',
'38',
'39',
'41',
'42',
'43',
'44',
'45',
'46',
'47',
'48',
'51',
'52',
'55',
'56',
'57',
'58',
'61',
'62',
'63',
'64',
'65',
'66',
'67',
'68',
'69',
'71',
'72',
'73',
'75',
'76',
'77',
'81',
'82',
'83',
'84',
'85',
'86',
'87',
'88',
'89',
'91',
'92',
'93',
'94',
'95',
'96',
'97',
'98',
'99']

code2_list = ['-',
'-',
'-',
'-',
'-',
'-',
'OA',
'-',
'-',
'SG',
'PD',
'PL',
'PE',
'PO',
'PH',
'PS',
'PC',
'PB',
'PW',
'CD',
'CP',
'CC',
'CI',
'CO',
'CU',
'CE',
'CL',
'CA',
'GD',
'GL',
'GE',
'GS',
'GC',
'GF',
'GB',
'GU',
'GT',
'TD',
'TM',
'TN',
'TS',
'TA',
'TC',
'TL',
'TV',
'DF',
'DG',
'ED',
'EC',
'EF',
'EO',
'FP',
'FF',
'FT',
'FS',
'FR',
'FL',
'FC',
'FA',
'FB',
'WO',
'WT',
'WR',
'WI',
'WS',
'WG',
'AT',
'AX',
'AE',
'AW',
'AS',
'AG',
'AF',
'AD',
'AM',
'RL',
'RT',
'RA',
'RS',
'RC',
'RO',
'MI',
'MO',
'MX']

with conn_postgres:
    equipage_count_df = pd.DataFrame()
    equipage_count_temp_4 = pd.DataFrame()
    print(code1_list)
    length_of_code_list = len(code1_list)
    print(length_of_code_list)
    for k in range(length_of_code_list):
        equipage_count_temp = count_dly(code1_list[k], code2_list[k])
        # print(equipage_count_temp)
        equipage_count_temp_4 = pd.concat([equipage_count_temp_4, equipage_count_temp], axis=0)
        #print(f"working on {year}{month}{day}")
        print(k)

equipage_count_df = pd.concat([equipage_count_temp_4])
# equipage_count_df = equipage_count_df.set_index(['time', 'dap'])
# print(equipage_count_df)
equipage_count_df.to_csv(f"/Users/pongabha/Desktop/dep_dly.csv")
