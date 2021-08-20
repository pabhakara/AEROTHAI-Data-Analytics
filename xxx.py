from pyproj import Transformer
import math

def convert_wgs_to_utm(lon: float, lat: float):
    """Based on lat and lng, return best utm epsg-code"""
    utm_band = str((math.floor((lon + 180) / 6 ) % 60) + 1)
    if len(utm_band) == 1:
        utm_band = '0'+utm_band
    if lat >= 0:
        epsg_code = '326' + utm_band
        return epsg_code
    epsg_code = '327' + utm_band
    return epsg_code


UTM_zone = convert_wgs_to_utm(12, 12)


transformer = Transformer.from_crs("epsg:4326", "epsg:"+str(UTM_zone))
transformer.transform(12, 12)