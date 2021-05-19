import psycopg2

try:
    conn_postgres = psycopg2.connect(user = "postgres",
                                  password = "password",
                                  host = "127.0.0.1",
                                  port = "5432",
                                  database = "flight_postgres")

    cursor = conn_postgres.cursor()

    year = "2019"

    for month in ["01","02","03","04","05","06","07","08","09","10","11","12"]:

        table_name = "target_2019_" + month + "_x"

        postgres_sql_text = "DROP TABLE IF EXISTS " + table_name + "; \n" + \
                            "SELECT a.*,b.dep,b.dest,b.actype,b.reg,b.wturb,b.frule,b.op_type, " + \
                            "ST_SetSRID(ST_MakePoint(a.longitude,a.latitude),4326)::geography as geog " +\
                            "INTO target_2019_" + month + "_x " \
                            "FROM target_2019_" + month + " a, " + "\"" + "2019_" + month + "_radar" + "\"" + " b " + \
                            "WHERE a.flight_id = b.flight_id;"

        print(postgres_sql_text)

        cursor.execute(postgres_sql_text)

        conn_postgres.commit()

except (Exception, psycopg2.Error) as error :
    print ("Error while connecting to PostgreSQL", error)
finally:
    #closing database connection.
        if(conn_postgres):
            cursor.close()
            conn_postgres.close()
            print("PostgreSQL connection is closed")
