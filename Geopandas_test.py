import geopandas
from sqlalchemy import create_engine
db_connection_url = "postgres://postgres:password@localhost:5432/flight_track"
con = create_engine(db_connection_url)
sql = "SELECT ST_AsText(geom),* from track_2019_12"
df = geopandas.read_postgis(sql, con)
df.head()