import mysql.connector
from mysql.connector import Error

db = mysql.connector.connect(host='localhost',
                             database='flight',
                             user='root',
                             password='password',
                             auth_plugin='mysql_native_password')
import psycopg2

encoding = 'Latin1'

dbx = db.cursor()
DB = psycopg2.connect("dbname='flight_target'")
DC = DB.cursor()
DC.execute("set client_encoding = " + encoding)

mysql = '''show tables from flight'''
dbx.execute(mysql)
ts = dbx.fetchall()
tables = []
for table in ts:
    tables.append(table[0])
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
        type = row[1].decode('ascii')
        print(type)
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
