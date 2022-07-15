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

#from dbname_and_paths import db_name,path_script,schema_name,airac

db_name = 'navigraph'
#db_name = 'test'
schema_name = 'public'

#airac_list = ['2106','2107','2108','2109','2110','2111','2112','2201','2202','2203']
airac_list = reversed(['2206'])

for airac in airac_list:

    path_script = "/Users/pongabha/Dropbox/Workspace/PycharmProjects/AEROTHAI_Data_Analytics/"

    path_db = '/Users/pongabha/Dropbox/Workspace/AEROTHAI Data Analytics/NavData/simtoolkitpro_native_' + airac +'/'

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

    print(db_name)
    print(schema_name)

    # #establishing the connection
    conn2 = psycopg2.connect(
        user='postgres',
        password='password',
        host='127.0.0.1',
        port='5432',
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
        database=db_name,
        user='postgres',
        password='password',
        host='127.0.0.1',
        port='5432'
    )

    conn3.autocommit = True
    cursor3 = conn3.cursor()

    sql_file = open(path_script + 'clean_up_legs.sql', 'r')
    cursor3.execute(sql_file.read())

    # Move the tables from PUBLIC SCHEMA to airac_xxx SCHEMA

    schema_name = f"airac_{airac}"

    postgres_sql_text = f"DROP SCHEMA IF EXISTS {schema_name} CASCADE;" \
                        f"CREATE SCHEMA {schema_name};" \
                        "DO " \
                        "$$ " \
                        "DECLARE " \
                        "row record; " \
                        "BEGIN " \
                        "FOR row IN SELECT tablename FROM pg_tables " \
                        "WHERE schemaname = 'public' and NOT(tablename like 'spat%') " \
                        "LOOP " \
                        f"EXECUTE 'DROP TABLE IF EXISTS {schema_name}.' || quote_ident(row.tablename) || ' ;'; " \
                        f"EXECUTE 'ALTER TABLE public.' || quote_ident(row.tablename) || ' SET SCHEMA {schema_name};'; " \
                        " END LOOP; " \
                        "END; " \
                        "$$;"

    cursor3.execute(postgres_sql_text)
    conn3.commit()
    #exec(open(path_script + 'Filter_Only_VT.py').read())
    conn3.close()

    # Create VT version of NavData

    conn_postgres = psycopg2.connect(
        user='postgres', password='password',
        host='127.0.0.1', port='5432',
        database=db_name,
        options="-c search_path=dbo," + schema_name
    )
    with conn_postgres:
        cursor_postgres = conn_postgres.cursor(cursor_factory=psycopg2.extras.DictCursor)

        postgres_sql_text = f" CREATE SCHEMA IF NOT EXISTS {schema_name}_vt; " \
                            " SELECT tablename FROM pg_tables " \
                            f" WHERE schemaname = '{schema_name}' " \
                            " AND NOT(tablename like '%head%') " \
                            " AND NOT(tablename like 'sbas%');"

        print(postgres_sql_text)
        cursor_postgres.execute(postgres_sql_text)
        table_name_list = cursor_postgres.fetchall()
        for table_name in table_name_list:
            print(table_name[0])
            postgres_sql_text = f" SELECT * " \
                                f" INTO {schema_name}_vt.{table_name[0]}" \
                                f" FROM {schema_name}.{table_name[0]}" \
                                f" WHERE public.ST_Intersects(geom," \
                                f" (SELECT public.ST_Buffer(geom,10) " \
                                f" FROM airspace.fir " \
                                f" WHERE name like 'BANGKOK%'));" # \
                                # f" DROP TABLE {schema_name}_vt.{table_name[0]};" \
                                # f" ALTER TABLE {schema_name}_vt.{table_name[0]}_vt RENAME TO {table_name[0]};"
            print(postgres_sql_text)
            cursor_postgres.execute(postgres_sql_text)
            conn_postgres.commit()

toc()