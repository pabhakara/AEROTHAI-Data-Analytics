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

from dbname_and_paths import db_name

path_script = "/Users/pongabha/Dropbox/Workspace/PycharmProjects/AEROTHAI_Data_Analytics/"
#path_db = '/Users/pongabha/Dropbox/Workspace/AEROTHAI Data Analytics/NavData/simtoolkitpro_native_2111/'
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

#Populating the database
exec(open(path_script + 'SQLite_File_to_PostgreSQL.py').read())

# #establishing the connection
conn2 = psycopg2.connect(
   database=db_name, user='postgres', password='password', host='127.0.0.1', port='5432'
)

conn2.autocommit = True
cursor2 = conn2.cursor()
sql_file = open(path_script + 'create_wp.sql', 'r')
cursor2.execute(sql_file.read())
conn2.close()
# # #

exec(open(path_script + 'Create_MORA_Grid_simtoolkit.py').read())
exec(open(path_script + 'Create_SID_Legs_simtoolkit.py').read())
exec(open(path_script + 'Create_STAR_Legs_simtoolkit.py').read())
exec(open(path_script + 'Create_IAP_Legs_RF_simtoolkit.py').read())
exec(open(path_script + 'Create_IAP_Legs_AF_simtoolkit.py').read())
exec(open(path_script + 'Create_IAP_Legs_simtoolkit.py').read())
exec(open(path_script + 'Create_ATS_Route_Segments_simtoolkit.py').read())
exec(open(path_script + 'Create_ATS_Route_simtoolkit.py').read())
exec(open(path_script + 'Create_Runway_Segments_simtoolkit.py').read())
# exec(open(path_script + 'Create_Holding_Legs.py').read())
# exec(open(path_script + 'Create_Holding_Legs_from_IAPs.py').read())

#establishing the connection
conn3 = psycopg2.connect(
   database=db_name, user='postgres', password='password', host='127.0.0.1', port='5432'
)

conn3.autocommit = True
cursor3 = conn3.cursor()
sql_file = open(path_script + 'clean_up_legs.sql', 'r')
cursor3.execute(sql_file.read())
conn3.close()
#

toc()