import snowflake.connector
import pandas as pd
import psycopg2.extras
from sqlalchemy import create_engine, URL

conn_postgres_target = psycopg2.connect(user="postgres",
                                        password="password",
                                        host="localhost",
                                        port="5432",
                                        database="temp",
                                        options="-c search_path=dbo,oag")

cur_postgres = conn_postgres_target.cursor()

conn_snowflake = snowflake.connector.connect(
    user='pabhakara@gmail.com',
    password='p2583547Por@snow',
    account='qo55348.ap-southeast-1',
    database='OAG_SCHEDULES',
)
cur_snowflake = conn_snowflake.cursor()

table_name = 'AERONAUTICAL_EMISTAT'

snowflake_sql_query = f"SELECT COLUMN_NAME,IS_NULLABLE,DATA_TYPE " \
            f"FROM INFORMATION_SCHEMA.COLUMNS " \
            f"WHERE TABLE_NAME = '{table_name}'"
x = cur_snowflake.execute(snowflake_sql_query)
cur_snowflake.execute(snowflake_sql_query)
df1 = cur_snowflake.fetch_pandas_all()
# record = cur.fetchall()
#

#
# postgres_sql_text = f"DROP TABLE IF EXISTS oag.{table_name};\n"
# postgres_sql_text += f"CREATE TABLE IF NOT EXISTS oag.{table_name} \n(" \
#
# num_of_columns = len(df1['COLUMN_NAME'])
#
# for k in range(0,num_of_columns-1):
#     postgres_sql_text += f"{df1['COLUMN_NAME'][k]} {df1['DATA_TYPE'][k]},\n"
# postgres_sql_text += f"{df1['COLUMN_NAME'][num_of_columns-1]} {df1['DATA_TYPE'][num_of_columns-1]});"
# postgres_sql_text = postgres_sql_text.replace('NUMBER','NUMERIC')
# postgres_sql_text = postgres_sql_text.replace('TIMESTAMP_NTZ','timestamp without time zone')
# cur_postgres.execute(postgres_sql_text)
# conn_postgres_target.commit()

date_list = pd.date_range(start='2023-08-01', end='2023-08-31')

for date in date_list:

    year = f"{date.year}"
    month = f"{date.month:02d}"
    day = f"{date.day:02d}"

    print(f'working on {year}-{month}-{day}')

    snowflake_sql_query = f"SELECT *" \
                f" FROM OAG_SCHEDULES.DIRECT_CUSTOMER_CONFIGURATIONS.{table_name}"\
                f" WHERE SCHEDULED_DEPARTURE_DATE = '{year}-{month}-{day}';"\

    x = cur_snowflake.execute(snowflake_sql_query)
    cur_snowflake.execute(snowflake_sql_query)
    df2 = cur_snowflake.fetch_pandas_all()
    #record = cur_snowflake.fetchall()
    print(df2)

    #engine = create_engine('postgresql+psycopg2://postgres:password@localhost:5432/temp')

    url_object = URL.create(
        "postgresql+psycopg2",
        username="postgres",
        password="password",  # plain (unescaped) text
        host="localhost",
        database="temp",
    )
    engine = create_engine(url_object)

    df2.to_sql(f'{table_name}_{year}{month}{day}', engine, if_exists='replace', index=False)

    postgres_sql_text = (f"DROP TABLE IF EXISTS oag.\"{table_name}_{year}{month}{day}\" ;"
                            f"ALTER TABLE public.\"{table_name}_{year}{month}{day}\" SET SCHEMA oag;")
    cur_postgres.execute(postgres_sql_text)
    conn_postgres_target.commit()

# num_of_columns = len(df1['COLUMN_NAME'])
# for k in range(0,num_of_columns-1):