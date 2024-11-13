import datetime as dt
import time
from subprocess import PIPE, Popen

import pandas as pd

import psycopg2.extras

import psycopg2

conn_postgres = psycopg2.connect(user="postgres",
                                 password="password",
                                 host="127.0.0.1",
                                 port="5432",
                                 database="temp",
                                 options="-c search_path=dbo,public")

with conn_postgres:
    cursor_postgres = conn_postgres.cursor(cursor_factory=psycopg2.extras.DictCursor)

    postgres_sql_text = (f"SELECT * FROM airac_current.stars_wp"
                         f" WHERE airport_identifier LIKE 'VT%'"
                         f" ORDER BY airport_identifier, procedure_identifier,route_type,transition_identifier,seqno ASC;")

    print(postgres_sql_text)

    cursor_postgres.execute(postgres_sql_text)
    record = cursor_postgres.fetchall()

    num_of_record = len(record)

    print(num_of_record)

    print(record)