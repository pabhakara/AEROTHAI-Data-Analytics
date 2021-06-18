import mysql.connector
import psycopg2
import time


t = time.time()

db = mysql.connector.connect(host='172.16.101.32',
                             database='flight',
                             user='pabhakara',
                             password='327146ra',
                             auth_plugin='mysql_native_password')

encoding = 'Latin1'

dbx = db.cursor()
DB = psycopg2.connect("dbname='flight_target'")
DC = DB.cursor()
DC.execute("set client_encoding = " + encoding)

mysql = '''show tables from flight'''
dbx.execute(mysql)
ts = dbx.fetchall()
tables = ['target_2021_05']

for table in tables:
    mysql = '''describe flight.%s''' % (table)
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

    msql = '''select * from flight.%s''' % (table)
    dbx.execute(msql)
    rows = dbx.fetchall()
    n = len(rows)
    print(n)
    t = n
    if n == 0: continue  # skip if no data

    cols = len(rows[0])
    print(rows)
    for row in rows:
        ps = ', '.join(['%s'] * cols)
        psql = '''insert into "%s" values(%s)''' % (table, ps)
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
