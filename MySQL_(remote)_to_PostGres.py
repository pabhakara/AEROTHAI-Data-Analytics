import psycopg2
import mysql.connector
import time

t = time.time()

db = mysql.connector.connect(host='172.16.101.32',
                             database='flight',
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

DB = psycopg2.connect("dbname='temp'")
DC = DB.cursor()
DC.execute("set client_encoding = " + encoding)

mysql_db = 'flight'
#mysql_db = 'flight_vtbd'

#mysql = '''show tables from flight_vtbd'''

mysql = 'show tables from ' + mysql_db


dbx.execute(mysql)
ts = dbx.fetchall()

# tables = ['2021_01_vtbs_tecos_arr','2021_02_vtbs_tecos_arr',
# '2021_03_vtbs_tecos_arr','2021_04_vtbs_tecos_arr',
# '2021_05_vtbs_tecos_arr','2021_06_vtbs_tecos_arr',
# '2021_07_vtbs_tecos_arr','2021_08_vtbs_tecos_arr',
# '2021_09_vtbs_tecos_arr','2021_10_vtbs_tecos_arr',
# '2021_11_vtbs_tecos_arr','2021_12_vtbs_tecos_arr']
tables = []



#prefix = ''
#postfix = '_vtbd_tecos_dep'

# prefix = ''
# postfix = '_radar'

# prefix = ''
# postfix = '_fdmc'

prefix = 'target_'
postfix = ''

# prefix = 'distances_'
# postfix = ''

# prefix = ''
# postfix = '_radar_position_at_fix'
years = ['2017']
#years = ['2015','2014','2013']

for year in years:
    #for month in ['01','02','03','04','05','06','07','08','09','10','11','12']:
    for month in ['05','06','07','08','09','10','11','12']:
        text = (prefix + year + '_' + month + postfix)
        print(text)
        tables = tables + [text]

for table in tables:
    mysql = 'describe '+ mysql_db + '.%s' % (table)
    dbx.execute(mysql)
    rows = dbx.fetchall()
    print(rows)
    # psql = 'drop table "%s' % (table) + '"'
    # DC.execute(psql)
    #DB.commit()

    psql = 'create table "%s"( ' % (table) + ''
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
        psql += '%s %s,' % (name, type)
    psql = psql.strip(',') + ')'
    print(psql)

    try: DC.execute(psql); DB.commit()
    except Exception as e:
        print(e)
        DB.rollback()

    msql = 'select * from '+ mysql_db  + '.%s' % (table)
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
        psql = '''insert into "%s" values(%s)''' % (table, ps)
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
