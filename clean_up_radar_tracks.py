import psycopg2
import psycopg2.extras
try:
    conn_postgres = psycopg2.connect(user = "postgres",
                                  password = "password",
                                  host = "127.0.0.1",
                                  port = "5432",
                                  database = "postgres")
    with conn_postgres.cursor(cursor_factory=psycopg2.extras.DictCursor) as cursor:
        cursor.execute(open("query_for_tracks_stats.sql", "r").read())
        record = cursor.fetchall()
        num_of_records = len(record)
        print("num_of_record: ", num_of_records)
        for row in record:
            dep = str(row['dep'])
            dest = str(row['dest'])
            percent_tile_01 = str(row['percent_tile_01'])
            postgres_sql_text = 'delete from radar_track_2019_z where not((track_length < ' + percent_tile_01 + \
                        ') or (track_length < 5) and dep = \''+ dep + '\' and dest = \'' + dest + '\''
            print(postgres_sql_text)
            cursor.execute(postgres_sql_text)
            conn_postgres.commit()
except (Exception, psycopg2.Error) as error :
    print ("Error while connecting to PostgreSQL", error)
