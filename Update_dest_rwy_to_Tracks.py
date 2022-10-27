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


date_list = pd.date_range(start='2022-10-23', end='2022-10-23')

with conn_postgres_target:
    date = date_list[0]
    year = f"{date.year}"
    month = f"{date.month:02d}"
    day = f"{date.day:02d}"
    yyyymmdd = f"{year}{month}{day:}"
    yyyymm = f"{year}{month}"

    cursor_postgres_target = conn_postgres_target.cursor(cursor_factory=psycopg2.extras.DictCursor)
    postgres_sql_text = f"ALTER TABLE track.track_cat62_{yyyymmdd} \n" \
                        f"DROP COLUMN IF EXISTS dest_rwy;\n" \
                        f"ALTER TABLE track.track_cat62_{yyyymmdd} \n" \
                        f"ADD COLUMN dest_rwy character varying(3) DEFAULT '-';\n"
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
        next_day = start_day + dt.timedelta(days=1)

        year_next = f"{next_day.year}"
        month_next = f"{next_day.month:02d}"
        day_next = f"{next_day.day:02d}"

        yyyymmdd_next = f"{year_next}{month_next}{day_next:}"
        yyyymm_next = f"{year_next}{month_next}"

        print(f"working on {yyyymmdd}")
        # Create an SQL query that selects surveillance targets from the source PostgreSQL database
        postgres_sql_text = f"UPDATE track.track_cat62_{yyyymmdd} t\n" \
                             f"SET dest_rwy = f.dest_rwy\n" \
                             f"FROM\n" \
                             f"(SELECT flight_key,dest_rwy\n" \
                             f"FROM\n" \
                             f"(SELECT flight_key,dest,dest_rwy,max(count)\n" \
                             f"FROM\n" \
                             f"(SELECT flight_key,dest, right(procedure_identifier,2) as dest_rwy,COUNT(*)\n" \
                             f"FROM\n" \
                             f"	(SELECT t.flight_key,t.position,t.vert,f.dest,b.airport_identifier,b.procedure_identifier\n" \
                             f"	FROM sur_air.cat062_{yyyymmdd} t, \n" \
                             f"	flight_data.flight_{yyyymm} f,\n" \
                             f"	temp.vt_finalpath_buffer b\n" \
                             f"	WHERE  t.flight_id = f.id\n" \
                             f"	AND (f.dest LIKE 'VT%')\n" \
                             f"	AND ST_INTERSECTS(t.position,b.final_buffer)\n" \
                             f"	UNION\n" \
                             f"	SELECT t.flight_key,t.position,t.vert,f.dest,b.airport_identifier,b.procedure_identifier\n" \
                             f"	FROM sur_air.cat062_{yyyymmdd_next} t, \n" \
                             f"	flight_data.flight_{yyyymm_next} f,\n" \
                             f"	temp.vt_finalpath_buffer b\n" \
                             f"	WHERE  t.flight_id = f.id\n" \
                             f"	AND (f.dest LIKE 'VT%')\n" \
                             f"	AND ST_INTERSECTS(t.position,b.final_buffer)\n" \
                             f"	) a\n" \
                             f"WHERE dest = airport_identifier AND vert = 2\n" \
                             f"AND length(procedure_identifier) = 3\n" \
                             f"GROUP BY flight_key,dest,procedure_identifier\n" \
                             f"UNION\n" \
                             f"SELECT flight_key,dest, right(procedure_identifier,3) as dest_rwy,COUNT(*) \n" \
                             f"FROM \n" \
                             f"	(SELECT t.flight_key,t.position,t.vert,f.dest,b.airport_identifier,b.procedure_identifier\n" \
                             f"	FROM sur_air.cat062_{yyyymmdd} t, \n" \
                             f"	flight_data.flight_{yyyymm} f,\n" \
                             f"	temp.vt_finalpath_buffer b\n" \
                             f"	WHERE  t.flight_id = f.id\n" \
                             f"	AND (f.dest LIKE 'VT%')\n" \
                             f"	AND ST_INTERSECTS(t.position,b.final_buffer)\n" \
                             f"	UNION\n" \
                             f"	SELECT t.flight_key,t.position,t.vert,f.dest,b.airport_identifier,b.procedure_identifier\n" \
                             f"	FROM sur_air.cat062_{yyyymmdd_next} t, \n" \
                             f"	flight_data.flight_{yyyymm_next} f,\n" \
                             f"	temp.vt_finalpath_buffer b\n" \
                             f"	WHERE  t.flight_id = f.id\n" \
                             f"	AND (f.dest LIKE 'VT%')\n" \
                             f"	AND ST_INTERSECTS(t.position,b.final_buffer)\n" \
                             f"	) a\n" \
                             f"WHERE dest = airport_identifier AND vert = 2\n" \
                             f"AND length(procedure_identifier) = 4\n" \
                             f"GROUP BY flight_key,dest,procedure_identifier) a\n" \
                             f"GROUP BY flight_key,dest,dest_rwy) b\n" \
                             f") f\n" \
                             f"WHERE  t.flight_key = f.flight_key;\n"
        cursor_postgres_target.execute(postgres_sql_text)
        conn_postgres_target.commit()

