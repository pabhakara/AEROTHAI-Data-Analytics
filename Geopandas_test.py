import geopandas
from sqlalchemy import create_engine
db_connection_url = "postgres://postgres:password@localhost:5432/flight_postgres"
con = create_engine(db_connection_url)
sql = "SELECT geom, * FROM track_2020_03_20_5sec"
df = geopandas.read_postgis(sql, con)
df.head()