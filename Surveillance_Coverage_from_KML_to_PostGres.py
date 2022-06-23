import psycopg2.extras
import geopandas as gpd
from shapely.wkb import dumps as wkb_dumps
import shapely

# Enable fiona driver
gpd.io.file.fiona.drvsupport.supported_drivers['KML'] = 'rw'

# Try to connect to the local PostGresSQL database in which we will store our surveillance coverage
conn_postgres = psycopg2.connect(user = "postgres",
                                  password = "password",
                                  host = "127.0.0.1",
                                  port = "5432",
                                  database = "surv_coverage")

#source_list = ['All','CMA','HTY','PSL','PUT','SVB','UBL','UDN','ROT','STN','CPN','CTR']
source_list = ['DMK']
type = 'SSR'
table_name = 'surv_coverage2'

with conn_postgres:
    # cursor_postgres = conn_postgres.cursor()
    # postgres_sql_text = "DELETE FROM surv_coverage WHERE source like \'"+ source_list[0] +"\';"
    # cursor_postgres.execute(postgres_sql_text)
    # conn_postgres.commit()

    for source in source_list:
        print(source)
        for fl in range(500,22000,500):
        #for fl in range(22500,50500,500):
            print(fl)
            filename = source + "_Radar_Coverage_"+ str(fl) +"FT.kml"
            #filename = 'coverage' + str(fl) + '.kml'

            path = f"/Users/pongabha/Dropbox/Workspace/Surveillance Enhancement/Radar Coverage" \
                   f"/Radar_Coverage/{source}_Radar_Coverage/{filename}"
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
            insert_query = f"""
                INSERT INTO {table_name} (site_id, type, flevel, geom)
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
    cursor_postgres = conn_postgres.cursor()
    postgres_sql_text = f"ALTER TABLE {table_name} " \
                        f"ALTER COLUMN geom " \
                        f"TYPE geometry(MultiPolygon, 4326) " \
                        f"USING ST_SetSRID(geom, 4326); "
    cursor_postgres.execute(postgres_sql_text)
    conn_postgres.commit()
