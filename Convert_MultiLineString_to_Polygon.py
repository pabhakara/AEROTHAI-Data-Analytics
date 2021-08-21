from shapely.geometry import Polygon, mapping

import geopandas as gpd
from sqlalchemy import create_engine
from geoalchemy2 import Geometry

engine = create_engine('postgresql://postgres:password@localhost:5432/dem')


def linestring_to_polygon(fili_shps):
    gdf = gpd.read_file(fili_shps) #LINESTRING
    geom = [x for x in gdf.geometry]
    all_coords = mapping(geom[0])['coordinates']
    lats = [x[1] for x in all_coords]
    lons = [x[0] for x in all_coords]
    polyg = Polygon(zip(lons, lats))
    return gpd.GeoDataFrame(index=[0], crs=gdf.crs, geometry=[polyg])

path = '/Users/pongabha/Dropbox/Workspace/DEM Thailand/Contour 150m/'

filename = 'Thailand_DEM_contour_300m.shp'

x = linestring_to_polygon(path + filename)


x.to_sql('xxx',engine,if_exists='append',index=False,dtype={'geom': Geometry('POLYGON', srid=4326)})