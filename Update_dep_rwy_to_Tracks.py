import psycopg2
import psycopg2.extras
import time
import datetime as dt
import pandas as pd


def none_to_null(etd):
    if etd == 'None':
        x = 'null'
    else:
        x = "'" + etd + "'"
    return x


# Create a connection to the remote PostGresSQL database from which we will retrieve our data for processing in Python

conn_postgres_target = psycopg2.connect(user="de_old_data",
                                        password="de_old_data",
                                        host="172.16.129.241",
                                        port="5432",
                                        database="aerothai_dwh",
                                        options="-c search_path=dbo,public")

# conn_postgres_target = psycopg2.connect(user = "postgres",
#                                   password = "password",
#                                   host = "127.0.0.1",
#                                   port = "5432",
#                                   database = "temp",
#                                   options="-c search_path=dbo,public")


date_list = pd.date_range(start='2022-05-01', end='2022-05-01')

with conn_postgres_target:
    date = date_list[0]
    year = f"{date.year}"
    month = f"{date.month:02d}"
    day = f"{date.day:02d}"
    yyyymmdd = f"{year}{month}{day:}"
    yyyymm = f"{year}{month}"

    cursor_postgres_target = conn_postgres_target.cursor(cursor_factory=psycopg2.extras.DictCursor)
    postgres_sql_text = f"ALTER TABLE track.track_cat62_{yyyymm} \n" \
                        f"DROP COLUMN IF EXISTS dep_rwy;\n" \
                        f"ALTER TABLE track.track_cat62_{yyyymm} \n" \
                        f"ADD COLUMN dep_rwy character varying(3) DEFAULT '-';\n"
    #print(postgres_sql_text)
    cursor_postgres_target.execute(postgres_sql_text)
    conn_postgres_target.commit()

    for date in date_list:
        year = f"{date.year}"
        month = f"{date.month:02d}"
        day = f"{date.day:02d}"
        t = time.time()
        yyyymmdd = f"{year}{month}{day:}"
        yyyymm = f"{year}{month}"

        start_day = dt.datetime.strptime(f"{year}-{month}-{day}", '%Y-%m-%d')
        previous_day = start_day + dt.timedelta(days=-1)

        year_previous = f"{previous_day.year}"
        month_previous = f"{previous_day.month:02d}"
        day_previous = f"{previous_day.day:02d}"

        yyyymmdd_previous = f"{year_previous}{month_previous}{day_previous:}"
        yyyymm_previous = f"{year_previous}{month_previous}"

        print(f"working on {yyyymmdd}")

        # postgres_sql_text = f"ALTER TABLE track.track_cat62_{yyyymmdd} \n" \
        #                     f"DROP COLUMN IF EXISTS dep_rwy;\n" \
        #                     f"ALTER TABLE track.track_cat62_{yyyymmdd} \n" \
        #                     f"ADD COLUMN dep_rwy character varying(3) DEFAULT '-';\n"
        # # print(postgres_sql_text)
        # cursor_postgres_target.execute(postgres_sql_text)
        # conn_postgres_target.commit()


        # Create an SQL query that selects surveillance targets from the source PostgreSQL database
        postgres_sql_text = f"UPDATE track.track_cat62_{yyyymm} t\n" \
                             f"SET dep_rwy = f.dep_rwy\n" \
                             f"FROM\n" \
                             f"(SELECT flight_key,dep_rwy\n" \
                             f"FROM\n" \
                             f"(SELECT flight_key,dep,dep_rwy,max(count)\n" \
                             f"FROM\n" \
                             f"(SELECT flight_key,dep, right(runway_identifier,2) as dep_rwy,COUNT(*)\n" \
                             f"FROM\n" \
                             f"	(SELECT t.flight_key,t.position,t.vert,f.dep,b.airport_identifier,b.runway_identifier\n" \
                             f"	FROM sur_air.cat062_{yyyymmdd} t, \n" \
                             f"	flight_data.flight_{yyyymm} f,\n" \
                             f"	temp.vt_dep_buffer b\n" \
                             f"	WHERE  t.flight_id = f.id\n" \
                             f"	AND (f.dep LIKE 'VT%')\n" \
                             f"	AND ST_INTERSECTS(t.position,b.buffer)\n" \
                             f"	UNION\n" \
                             f"	SELECT t.flight_key,t.position,t.vert,f.dep,b.airport_identifier,b.runway_identifier\n" \
                             f"	FROM sur_air.cat062_{yyyymmdd_previous} t, \n" \
                             f"	flight_data.flight_{yyyymm_previous} f,\n" \
                             f"	temp.vt_dep_buffer b\n" \
                             f"	WHERE  t.flight_id = f.id\n" \
                             f"	AND (f.dep LIKE 'VT%')\n" \
                             f"	AND ST_INTERSECTS(t.position,b.buffer)\n" \
                             f"	) a\n" \
                             f"WHERE dep = airport_identifier AND vert = 1\n" \
                             f"AND length(runway_identifier) = 4\n" \
                             f"GROUP BY flight_key,dep,runway_identifier\n" \
                             f"UNION\n" \
                             f"SELECT flight_key,dep, right(runway_identifier,3) as dep_rwy,COUNT(*) \n" \
                             f"FROM \n" \
                             f"	(SELECT t.flight_key,t.position,t.vert,f.dep,b.airport_identifier,b.runway_identifier\n" \
                             f"	FROM sur_air.cat062_{yyyymmdd} t, \n" \
                             f"	flight_data.flight_{yyyymm} f,\n" \
                             f"	temp.vt_dep_buffer b\n" \
                             f"	WHERE  t.flight_id = f.id\n" \
                             f"	AND (f.dep LIKE 'VT%')\n" \
                             f"	AND ST_INTERSECTS(t.position,b.buffer)\n" \
                             f"	UNION\n" \
                             f"	SELECT t.flight_key,t.position,t.vert,f.dep,b.airport_identifier,b.runway_identifier\n" \
                             f"	FROM sur_air.cat062_{yyyymmdd_previous} t, \n" \
                             f"	flight_data.flight_{yyyymm_previous} f,\n" \
                             f"	temp.vt_dep_buffer b\n" \
                             f"	WHERE  t.flight_id = f.id\n" \
                             f"	AND (f.dep LIKE 'VT%')\n" \
                             f"	AND ST_INTERSECTS(t.position,b.buffer)\n" \
                             f"	) a\n" \
                             f"WHERE dep = airport_identifier AND vert = 1\n" \
                             f"AND length(runway_identifier) = 5\n" \
                             f"GROUP BY flight_key,dep,runway_identifier) a\n" \
                             f"GROUP BY flight_key,dep,dep_rwy) b\n" \
                             f") f\n" \
                             f"WHERE  t.flight_key = f.flight_key;\n"
        #print(postgres_sql_text)
        cursor_postgres_target.execute(postgres_sql_text)
        conn_postgres_target.commit()

