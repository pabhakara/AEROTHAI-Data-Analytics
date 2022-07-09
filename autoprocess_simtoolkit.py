import psycopg2

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
tic()

from dbname_and_paths import db_name,path_script,schema_name

#
# # #establishing the connection
# conn = psycopg2.connect(
#    database="postgres", user='postgres', password='password', host='127.0.0.1', port= '5432'
# )
# conn.autocommit = True
# #Creating a cursor object using the cursor() method
# cursor = conn.cursor()
# #Preparing query to create a database
# sql = 'CREATE database ' + db_name + ';'
# #Creating a database
# cursor.execute(sql)
# print("Database created successfully........")
# conn.close()

#Populating the database with simtoolkit navdata from sqlite file
exec(open(path_script + 'SQLite_File_to_PostgreSQL.py').read())

# #establishing the connection
conn2 = psycopg2.connect(
    user='postgres', password='password',
    host='127.0.0.1', port='5432',
    database=db_name,
    options="-c search_path=dbo," + schema_name
)

# conn2 = psycopg2.connect(user = "de_old_data",
#                                   password = "de_old_data",
#                                   host = "172.16.129.241",
#                                   port = "5432",
#                                   database = "aerothai_dwh",
#                                   options="-c search_path=dbo," + schema_name)

print(schema_name)
conn2.autocommit = True
cursor2 = conn2.cursor()
# #sql_file = open(path_script + 'create_wp_with_airac.sql', 'r')
#
sql_file = open(path_script + 'create_wp.sql', 'r')
cursor2.execute(sql_file.read())
conn2.close()
# #

exec(open(path_script + 'Create_MORA_Grid_simtoolkit.py').read())

exec(open(path_script + 'Create_SID_Legs_simtoolkit.py').read())
exec(open(path_script + 'Create_SID_Legs_RF_simtoolkit.py').read())
exec(open(path_script + 'Create_SID_Legs_simtoolkit_without_RF.py').read())

exec(open(path_script + 'Create_STAR_Legs_simtoolkit.py').read())
exec(open(path_script + 'Create_STAR_Legs_RF_simtoolkit.py').read())
exec(open(path_script + 'Create_STAR_Legs_simtoolkit_without_RF.py').read())

exec(open(path_script + 'Create_IAP_Legs_RF_simtoolkit.py').read())
exec(open(path_script + 'Create_IAP_Legs_AF_simtoolkit.py').read())
exec(open(path_script + 'Create_IAP_Legs_simtoolkit.py').read())

exec(open(path_script + 'Create_ATS_Route_Segments_simtoolkit.py').read())
exec(open(path_script + 'Create_ATS_Route_simtoolkit.py').read())
exec(open(path_script + 'Create_Runway_Segments_simtoolkit.py').read())
exec(open(path_script + 'Create_Holding_Legs.py').read())
exec(open(path_script + 'Create_Holding_Legs_from_IAPs.py').read())

#establishing the connection
conn3 = psycopg2.connect(
   database=db_name, user='postgres', password='password', host='127.0.0.1', port='5432'
)

conn3.autocommit = True
cursor3 = conn3.cursor()

sql_file = open(path_script + 'clean_up_legs.sql', 'r')
cursor3.execute(sql_file.read())
conn3.close()

# sql_file = open(path_script + 'clean_up_legs_vt.sql', 'r')
# cursor3.execute(sql_file.read())
# conn3.close()
# exec(open(path_script + 'Filter_Only_VT.py').read())


toc()