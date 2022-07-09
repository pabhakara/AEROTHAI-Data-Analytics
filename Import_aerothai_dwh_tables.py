import sys
import psycopg2

db = psycopg2.connect("")

conn_postgres_source = psycopg2.connect(user="de_old_data",
                                        password="de_old_data",
                                        host="172.16.129.241",
                                        port="5432",
                                        database="aerothai_dwh",
                                        options="-c search_path=dbo,sur_air")

with conn_postgres_source:
    cursor_postgres_source = conn_postgres_source.cursor(cursor_factory=psycopg2.extras.DictCursor)

    with open('myfile.csv', 'w') as f:
        with db.cursor() as cursor:
            cursor.copy_expert(
                """copy (select * from pg_timezone_names) to stdout with csv header""",
                file=f,
                size=256 * 1024
            )