import psycopg2

try:
    conn_postgres = psycopg2.connect(user = "postgres",
                                  password = "password",
                                  host = "127.0.0.1",
                                  port = "5432",
                                  database = "postgres")

    #cursor = conn_postgres.cursor()


    # table_name = "target_2019_+" +  +"_x"

    # postgres_sql_text = "DROP TABLE IF EXISTS " + table_name + "; \n" + \
    #                     "SELECT a.*,b.dep,b.dest,b.actype,b.reg,b.wturb,b.frule,b.ftype," + \
    #                     "(ST_MakePoint(a.longitude,a.latitude),4326) as geom " +\
    #                     "(callsign character varying, flight_id integer, geom geometry, " + \
    #                     "start_time timestamp without time zone, " + \
    #                     "etd timestamp without time zone, " + \
    #                     "atd timestamp without time zone, " + \
    #                     "eta timestamp without time zone, " + \
    #                     "ata timestamp without time zone, " + \
    #                     "end_time timestamp without time zone, " + \
    #                     "dep character varying, dest character varying, " + \
    #                     "actype character varying, " + \
    #                     "runway character varying, " + \
    #                     "sidstar character varying, " + \
    #                     "op_type character varying, " + \
    #                     "frule character varying, " + \
    #                     "reg character varying, " + \
    #                     "pbn_type character varying, " + \
    #                     "entry_flevel integer, " + \
    #                     "maintain_flevel integer, " + \
    #                     "exit_flevel integer, " + \
    #                     "comnav character varying, flevel integer,route character varying)" + \
    #                     "WITH (OIDS=FALSE); \n" + \
    #                     "ALTER TABLE " + table_name + " " \
    #                     "OWNER TO postgres;";
    # print(postgres_sql_text)
    # cursor.execute(postgres_sql_text)
    #
    # conn_postgres.commit()

    with conn_postgres.cursor() as cursor:
        cursor.execute(open("scratch.txt", "r").read())
        record = cursor.fetchall()
        num_of_records = len(record)
        print("num_of_record: ", num_of_records)

except (Exception, psycopg2.Error) as error :
    print ("Error while connecting to PostgreSQL", error)
finally:
    #closing database connection.
        if(conn_postgres):
            cursor.close()
            conn_postgres.close()
            print("PostgreSQL connection is closed")
