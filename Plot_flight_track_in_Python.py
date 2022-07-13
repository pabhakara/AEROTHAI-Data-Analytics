from sqlalchemy import create_engine
import geopandas
import psycopg2.extras
import matplotlib.pyplot as plt

username = "postgres"
password = "password"
host= "localhost"
port = "5432"
dbname = 'temp'

year = '2022'
month = '05'
day = '02'

conn_postgres_source = psycopg2.connect(user="postgres",
                                             password="password",
                                             host="localhost",
                                             port="5432",
                                             database="temp",
                                             options="-c search_path=dbo,sur_air")

output_filepath = '/Users/pongabha/Dropbox/Workspace/AEROTHAI Data Analytics/Flight_Proflie_Plots/'


with conn_postgres_source:
    cursor_postgres_source = conn_postgres_source.cursor(cursor_factory=psycopg2.extras.DictCursor)
    # Create an SQL query that selects the list of flights that we want plot
    # from the source PostgreSQL database
    postgres_sql_text = f"SELECT DISTINCT t.flight_key " \
                        f"FROM sur_air.cat062_{year}{month}{day} t " \
                        f"LEFT JOIN flight_data.flight_{year}{month} f " \
                        f"ON t.flight_id = f.id " \
                        f"WHERE (f.dep LIKE 'VTCC%' AND f.dest LIKE 'VTBS%') " \
                        f"AND t.acid LIKE '%' " \
                        f"AND f.frule LIKE 'I'; "
    cursor_postgres_source.execute(postgres_sql_text)
    flight_key_list = cursor_postgres_source.fetchall()
    print(flight_key_list)

for flight_key in flight_key_list:

    print(flight_key)

    db_connection_url = f"postgresql://{username}:{password}@{host}:{port}/{dbname}"
    con = create_engine(db_connection_url)
    # postgres_sql = f"SELECT t.flight_key, t.app_time, t.sector, t.dist_from_last_position, t.measured_fl, \n" \
    #                f"(t.\"2d_position\") as geom \n" \
    #                             f"FROM sur_air.cat062_{year}{month}{day} t \n" \
    #                             f"LEFT JOIN flight_data.flight_{year}{month} f \n" \
    #                             f"ON t.flight_id = f.id \n" \
    #                             f"LEFT JOIN airac_current.airports a \n" \
    #                             f"ON a.airport_identifier = f.dest \n" \
    #                             f"WHERE f.flight_key = '{flight_key[0]}' \n" \
    #                             f"ORDER BY t.app_time; "

    postgres_sql = f"SELECT t.flight_key, t.geom \n" \
                                f"FROM track.track_cat62_11_days t \n" \
                                f"WHERE t.flight_key = '{flight_key[0]}' \n" \

    gdf = geopandas.read_postgis(postgres_sql, con, geom_col='geom')

    gdf.plot(marker='.', markersize=1)
    plt.title(f"Flight_Key {flight_key[0]}")
    plt.xticks(rotation=90)
    # plt.set_xlim([90, 120])
    # plt.set_ylim([0, 25])
    # plt.legend()
    plt.grid()
    plt.tight_layout()
    plt.savefig(f"{output_filepath}{flight_key[0]}_track.png")
