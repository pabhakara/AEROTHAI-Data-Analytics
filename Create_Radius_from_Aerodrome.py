import psycopg2.extras

# conn_postgres_target = psycopg2.connect(user = "postgres",
#                                   password = "password",
#                                   host = "127.0.0.1",
#                                   port = "5432",
#                                   database = "temp",
#                                   options="-c search_path=dbo,public")

conn_postgres_target = psycopg2.connect(user="de_old_data",
                                             password="de_old_data",
                                             host="172.16.129.241",
                                             port="5432",
                                             database="aerothai_dwh",
                                             options="-c search_path=dbo,public")


with conn_postgres_target:
    cursor_postgres_target = conn_postgres_target.cursor()
    postgres_sql_text = f" CREATE OR REPLACE FUNCTION get_utmzone(input_geom geometry) \n"\
                        f"   RETURNS integer AS \n"\
                        f" $BODY$ \n"\
                        f" DECLARE \n"\
                        f"    zone int; \n"\
                        f"    pref int; \n"\
                        f" BEGIN "\
                        f"    IF GeometryType(input_geom) != 'POINT' THEN \n"\
                        f"      RAISE EXCEPTION 'Input geom must be a point. Currently is: %', GeometryType(input_geom); \n"\
                        f"    END IF; \n"\
                        f"    IF ST_Y(input_geom) >0 THEN \n"\
                        f"       pref:=32600; \n"\
                        f"    ELSE \n"\
                        f"       pref:=32700; \n"\
                        f"    END IF; \n"\
                        f"    zone = floor((ST_X(input_geom)+180)/6)+1; \n"\
                        f"    RETURN zone+pref; \n"\
                        f" END; \n"\
                        f" $BODY$ \n"\
                        f" LANGUAGE plpgsql IMMUTABLE; \n"
    cursor_postgres_target.execute(postgres_sql_text)
    conn_postgres_target.commit()

    postgres_sql_text = f" DROP TABLE IF EXISTS airac_current.airports_buffer; \n" \
                        f" SELECT * "
    for radius in range(10,510,10):
        postgres_sql_text += f", ST_Transform(ST_Buffer(ST_Transform(geom,get_utmzone(geom)),{radius}*1852,100),4326)\n " \
                             f" as nm{radius} "

    postgres_sql_text += f" INTO airac_current.airports_buffer \n"\
                         f" FROM airac_current.airports \n"\
                         f" WHERE icao_code = 'VT'; \n"
    cursor_postgres_target.execute(postgres_sql_text)
    conn_postgres_target.commit()