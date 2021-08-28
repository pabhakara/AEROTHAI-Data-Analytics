import psycopg2

# #establishing the connection
# conn = psycopg2.connect(
#    database="postgres", user='postgres', password='password', host='127.0.0.1', port= '5432'
# )
# conn.autocommit = True
#
# #Creating a cursor object using the cursor() method
# cursor = conn.cursor()

# import psycopg2

db_name = 'temp3'
#
# #Preparing query to create a database
# sql = 'CREATE database ' + db_name + ';'
#
# #Creating a database
# cursor.execute(sql)
# print("Database created successfully........")
# conn.close()

path_script = "/Users/pongabha/Dropbox/Workspace/PycharmProjects/AEROTHAI_Data_Analytics/"
path_db = '/Users/pongabha/Dropbox/Workspace/AEROTHAI Data Analytics/NavData/simtoolkitpro_native_2108/'

# #Populating the database
# exec(open(path_script + 'SQLite_File_to_PostgreSQL.py').read())
#
# #establishing the connection
# conn2 = psycopg2.connect(
#    database=db_name, user='postgres', password='password', host='127.0.0.1', port='5432'
# )
# conn2.autocommit = True
# cursor2 = conn2.cursor()
# sql_file = open(path_script + 'create_wp.sql', 'r')
# cursor2.execute(sql_file.read())
# conn2.close()
#
#
# exec(open(path_script + 'Create_MORA_Grid_simtoolkit.py').read())
# exec(open(path_script + 'Create_SID_Legs_simtoolkit.py').read())
# exec(open(path_script + 'Create_STAR_Legs_simtoolkit.py').read())
# exec(open(path_script + 'Create_IAP_Legs_RF_simtoolkit.py').read())
# exec(open(path_script + 'Create_IAP_Legs_AF_simtoolkit.py').read())
# exec(open(path_script + 'Create_IAP_Legs_simtoolkit.py').read())
# exec(open(path_script + 'Create_ATS_Route_Segments_simtoolkit.py').read())
# exec(open(path_script + 'Create_ATS_Route_simtoolkit.py').read())
exec(open(path_script + 'Create_Runway_Segments_simtoolkit.py').read())
exec(open(path_script + 'Create_Holding_Legs_from_IAPs.py').read())
exec(open(path_script + 'Create_Holding_Legs.py').read())
