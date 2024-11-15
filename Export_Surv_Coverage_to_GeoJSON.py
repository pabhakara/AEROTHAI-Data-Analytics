import os
import psycopg2.extras

db_name = 'surv_coverage'

schema_name = 'public'

directory_path = f"/Users/pongabha/Dropbox/Workspace/Surveillance Enhancement/" \
                   f"Simulated Coverage/geojson/"

# Create target Directory if don't exist
if not os.path.exists(directory_path):
    os.mkdir(directory_path)
else:
    os.remove(directory_path)
    os.mkdir(directory_path)

conn_postgres = psycopg2.connect(
        user='postgres', password='password',
        host='127.0.0.1', port='5432',
        database=db_name,
        options="-c search_path=dbo," + schema_name
    )

with conn_postgres:
    cursor_postgres = conn_postgres.cursor(cursor_factory=psycopg2.extras.DictCursor)

    postgres_sql_text = f"SELECT relname FROM pg_class " \
                        f"WHERE relnamespace = '{schema_name}'::regnamespace " \
                        "AND relkind = 'r' " \
                        "AND NOT (relname LIKE 'sbas%') " \
                        "AND NOT (relname LIKE 'tbl_%'); "

    cursor_postgres.execute(postgres_sql_text)
    table_list = cursor_postgres.fetchall()

    for table in table_list:
        table_name = table[0]
        print(table_name)
        os.system(f"ogr2ogr -f \"GeoJSON\" \"{directory_path}{table_name}.geojson\" "
                  f"PG:\"host=localhost dbname={db_name} user=postgres password=password port=5432\" "
                  f"\"{schema_name}.{table_name}(geom)\"")

schema_name = 'public'

conn_postgres = psycopg2.connect(
    user='postgres', password='password',
    host='127.0.0.1', port='5432',
    database=db_name,
    options="-c search_path=dbo," + schema_name
)

with conn_postgres:
    postgres_sql_text = f"SELECT relname FROM pg_class " \
                        f"WHERE relnamespace = '{schema_name}'::regnamespace " \
                        "AND relkind = 'r'; "
    cursor_postgres.execute(postgres_sql_text)
    table_list = cursor_postgres.fetchall()

    for table in table_list:
        table_name = table[0]
        print(table_name)
        os.system(f"ogr2ogr -f \"GeoJSON\" \"{directory_path}{table_name}.geojson\" "
                  f"PG:\"host=localhost dbname={db_name} user=postgres password=password port=5432\" "
                  f"\"{schema_name}.{table_name}(geom)\"")
