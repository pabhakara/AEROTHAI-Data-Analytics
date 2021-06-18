import os
import pyodbc
import csv

dataFile = "ASTNAV.mdb"

# set up some constants
MDB = os.getcwd() + "/" + dataFile
DRV = '{Microsoft Access Driver (*.mdb, *.accdb)}'
PWD = ''

# connect to db
con = pyodbc.connect('DRIVER={};DBQ={};PWD={}'.format(DRV,MDB,PWD))
cur = con.cursor()

# run a query and get the results
SQL = 'SELECT * FROM mytable;' # your query goes here
rows = cur.execute(SQL).fetchall()
cur.close()
con.close()

# you could change the mode from 'w' to 'a' (append) for any subsequent queries
with open('mytable.csv', 'wb') as fou:
    csv_writer = csv.writer(fou) # default field-delimiter is ","
    csv_writer.writerows(rows)