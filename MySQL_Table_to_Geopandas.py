import mysql.connector
import time
import pandas as pd


t = time.time()

conn_mysql = mysql.connector.connect(host='172.16.101.32',
                             database='flight',
                             user='pabhakara',
                             password='327146ra',
                             auth_plugin='mysql_native_password')

cursor_mysql = conn_mysql.cursor()

#years = ['2014','2015','2016','2017','2018','2019','2020']
months = ['01','02','03','04','05','06','07','08','09','10','11','12']

years = ['2018','2020']
#months = ['01','02']

df = pd.DataFrame([])

for year in years:
    for month in months:
        year_month = year + '_' + month
        print(year_month + '\n')
        mysql = 'select a.app_time,a.dist_from_last_position, b.actype ' + \
                'from target_' + year_month + ' a, ' + year_month + '_radar b ' + \
                'where a.flight_id = b.flight_id and b.actype like \'B78%\''
        cursor_mysql.execute(mysql)
        x = cursor_mysql.fetchall()
        temp = pd.DataFrame.from_records(x)
        df = df.append(temp)
        print(df.size)
print(df)
