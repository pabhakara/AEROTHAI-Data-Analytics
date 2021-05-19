import psycopg2.extras
import geopandas as gpd
from shapely.wkb import dumps as wkb_dumps
import shapely

# Enable fiona driver
gpd.io.file.fiona.drvsupport.supported_drivers['KML'] = 'rw'


# Try to connect to the local PostGresSQL database in which we will store our surveillance coverage

#engine = create_engine('postgresql://postgres:password@127.0.0.1:5432/flight_track_5s')

conn_postgres = psycopg2.connect(user = "postgres",
                                  password = "password",
                                  host = "127.0.0.1",
                                  port = "5432",
                                  database = "surv_coverage")

#Donmueang Radar coverage  500 Feet.kml

source = 'DMK'
type = 'SSR'

with conn_postgres:

    for fl in range(500,20500,500):
        print(fl)
        filename = 'Donmueang Radar coverage  '+ str(fl) +' Feet.kml'
        #filename = 'coverage' + str(fl) + '.kml'
        path = '/Users/pongabha/Dropbox/Workspace/AEROTHAI Data Analytics/Radar Coverage/Donmueang Radar coverage/'+ filename
        #All Radar coverage 500.kml'

        # Read file
        gdf = gpd.read_file(path, driver='KML')

        # Drop Z dimension of polygons that occurs often in kml - * expansion handles xy anx xyz cases

        gdf = gdf.set_geometry(
            gdf.geometry.map(
                lambda polygon: shapely.ops.transform(lambda x, y, *_: (x, y), polygon)
            )
        )

        # Copy the gdf if you want to keep the original intact
        insert_gdf = gdf.copy()

        # Make a new field containing the WKB dumped from the geometry column, then turn it into a regular
        insert_gdf["geom_wkb"] = insert_gdf["geometry"].apply(lambda x: wkb_dumps(x))

        # Define an insert query which will read the WKB geometry and cast it to GEOMETRY type accordingly
        insert_query = """
            INSERT INTO surv_coverage (source, type, flevel, geom)
            VALUES (%(source)s, %(type)s, %(flevel)s, ST_GeomFromWKB(%(geom_wkb)s));
        """

        # Build a list of execution parameters by iterating through the GeoDataFrame
        # This is considered bad practice by the pandas community because it is slow.
        params_list = [
            {
                "source": source,
                "type": type,
                "flevel": str(fl),
                "geom_wkb": row["geom_wkb"]
            } for i, row in insert_gdf.iterrows()
        ]
        # Connect to the database and make a cursor
        cur = conn_postgres.cursor()

        # Iterate through the list of execution parameters and apply them to an execution of the insert query
        for params in params_list:
            cur.execute(insert_query, params)

