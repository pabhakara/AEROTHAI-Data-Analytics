import mysql.connector
import psycopg2
import time

mysql_db = 'flight_vtsp'

#from magic import Connect #Private mysql connect information - I COMMENTED THIS LINE to use direct connection
db = mysql.connector.connect(host='172.16.101.32',
                             database= mysql_db,
                             user='pabhakara',
                             password='327146ra',
                             auth_plugin='mysql_native_password')

# set client_encoding if different that PostgreSQL database default
encoding = 'Latin1'

dbx=db.cursor()
DB=psycopg2.connect("dbname='tecos'")
DC=DB.cursor()
DC.execute("set client_encoding = " + encoding)

mysql='show tables from ' + mysql_db
dbx.execute(mysql); ts=dbx.fetchall(); tables=[]
for table in ts: tables.append(table[0])
for table in tables:
    mysql='''describe flight_vtsp.%s'''%(table)
    dbx.execute(mysql); rows=dbx.fetchall()
    psql='drop table if exists \"' + table +  '\"'
    DC.execute(psql); DB.commit()

    psql='create table \"' + table + '\"('
    for row in rows:
        name=row[0]; type=row[1]
        if 'char' in type: type = 'character varying'
        if 'int' in type: type = 'integer'
        if 'double' in type: type = 'double precision'
        if 'datetime' in type: type = 'timestamp without time zone'
        if 'varchar' in type: type = 'character varying'
        if 'enum' in type:
            type = 'varchar'
            print ("warning : conversion of enum to varchar %s(%s)" % (table, name))
        psql+='%s %s,'%(name,type)
    psql = psql.strip(',')+')'
    print(psql)
    try: DC.execute(psql); DB.commit()
    except Exception as e:
        print(e)
        DB.rollback()

    msql = '''select * from flight_vtsp.%s''' % (table)
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