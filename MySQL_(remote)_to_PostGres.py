import psycopg2
import mysql.connector
import time
import pandas as pd

t = time.time()

# mysql_db = 'flight'
mysql_db = 'flight_vtbd'

db = mysql.connector.connect(host='172.16.101.32',
                             database=mysql_db,
                             user='pabhakara',
                             password='327146ra',
                             auth_plugin='mysql_native_password')
encoding = 'Latin1'

dbx = db.cursor()

# DB = psycopg2.connect(user = "de_old_data",
#                                   password = "de_old_data",
#                                   host = "172.16.129.241",
#                                   port = "5432",
#                                   database = "aerothai_dwh",
#                                   options="-c search_path=dbo,flight_data")

# DB = psycopg2.connect(database="temp",options="-c search_path=dbo,flight_data")

DB = psycopg2.connect(user="postgres",
                                 password="password",
                                 host="localhost",
                                 port="5432",
                                 database="temp",
                                 options="-c search_path=dbo,tecos")

DC = DB.cursor()
DC.execute("set client_encoding = " + encoding)

#mysql = '''show tables from flight_vtbd'''

mysql = f'show tables from {mysql_db}'

dbx.execute(mysql)
ts = dbx.fetchall()

tables = []
#
prefix = ''
postfix = '_vtbd_tecos_dep'

# prefix = ''
# postfix = '_radar'

# prefix = ''
# postfix = '_fdmc'

#prefix = 'target_'
#postfix = ''

# prefix = 'distances_'
# postfix = ''

# prefix = ''
# postfix = '_radar_position_at_fix'

# year_list_3 = ['2022','2021','2020','2019', '2018', '2017']
# month_list_3 = ['01','02','03','04','05','06','07','08','09','10','11','12']

# year_list_3 = ['2023']
# month_list_3 = ['01','02','03','04','05','06']
date_list = pd.date_range(start='2025-01-01', end='2025-03-31',freq = 'M')

print(date_list)
# today = dt.datetime.now()
# date_list = [dt.datetime.strptime(f"{today.year}-{today.month}-{today.day}", '%Y-%m-%d') + dt.timedelta(days=-3)]

for date in date_list:
    year = f"{date.year}"
    month = f"{date.month:02d}"

    text = f"{prefix}{year}_{month}{postfix}"
    print(text)
    tables = tables + [text]

for table in tables:
    mysql = f'describe {mysql_db}.{table}'
    dbx.execute(mysql)
    rows = dbx.fetchall()
    print(rows)
    # psql = 'drop table "%s' % (table) + '"'
    # DC.execute(psql)
    #DB.commit()

    psql = f'drop table if exists "{table}"; create table "{table}" ('
    for row in rows:
        name = row[0]
        type = row[1] #.decode('ascii')
        if 'char' in type: type = 'character varying'
        if 'int' in type: type='integer'
        if 'double' in type: type='double precision'
        if 'datetime' in type: type='timestamp without time zone'
        if 'varchar' in type: type='character varying'
        if 'enum' in type:
            print("warning : conversion of enum to varchar %s(%s)" % (table, name))
        psql += f'{name} {type},'
    psql = psql.strip(',') + ')'
    print(psql)

    try: DC.execute(psql); DB.commit()
    except Exception as e:
        print(e)
        DB.rollback()

    #msql = f'select * from '+ mysql_db  + '.%s' % (table)
    msql = f'select * from {mysql_db}.{table}'
    dbx.execute(msql)
    rows = dbx.fetchall()
    n = len(rows)
    print(n)
    t = n
    if n == 0: continue  # skip if no data

    cols = len(rows[0])

    for row in rows:
        print(row)
        ps = ', '.join(['%s'] * cols)
        psql = f'insert into "{table}" values({ps})'
        print(psql)
        DC.execute(psql, (row))
        n = n - 1
        if n % 1000 == 1: DB.commit();

    DB.commit()
    try:
        DC.execute(psql)
        DB.commit()
    except Exception as e:
        print(e)
        DB.rollback()
