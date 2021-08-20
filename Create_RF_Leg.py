import numpy
import psycopg2
from functools import partial
from pyproj import Proj, transform
import math
import numpy as np

import utm
import haversine as hs

from pyproj import Transformer

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


table_name = 'airways'

conn = psycopg2.connect(user = "postgres",
                        password = "password",
                        host = "127.0.0.1",
                        port = "5432",
                        database = "airac_2021_08_simtoolkit")

with conn:
    cur = conn.cursor()

    AR691 = [-17.95237222, 65.80899167]
    AR692 = [-17.99399722, 65.85554167]
    AR693 = [-18.07259444, 65.85552222]
    ARI69 = [-18.12053333, 65.81853889]
    #
    AR694 = [-18.03325, 65.82322778]

    HK870 = [8.18516667, 98.21938611]
    SAMON = [8.10702222, 98.25089722]
    RHK87 = [8.15073889, 98.24674722]

    Turn = 'L'

    SP801 = [7.80369722, 98.47637222]
    SAABA = [7.98402222, 98.2681]

    HK680 = [8.02243611, 98.22369167]
    SAMON = [8.10702222, 98.25089722]
    RHK68 = [8.058925, 98.25546111]

    UTM_zone = convert_wgs_to_utm(SAABA[1],SAABA[0])

    proj_4326 = Proj(init='epsg:4326')
    proj_UTM = Proj(init='epsg:' + str(UTM_zone))

    IF_wp_latlong = SP801
    WP2_wp_latlong = SAABA

    start_wp_latlong = HK680
    end_wp_latlong = SAMON
    arc_center_latlong = RHK68

    transformer = partial(transform, proj_4326, proj_UTM)

    transformer = Transformer.from_crs("epsg:4326", "epsg:"+ UTM_zone)
    #transformer.transform(12, 12)

    #transformer = Transformer.from_crs("epsg:4326", "epsg:3857")

    IF_wp_xy = transformer.transform(IF_wp_latlong[1], IF_wp_latlong[0])

    IF_wp_xy = utm.from_latlon(IF_wp_latlong[0], IF_wp_latlong[1])

    WP2_wp_xy = utm.from_latlon(WP2_wp_latlong[0], WP2_wp_latlong[0])
    start_wp_xy = utm.from_latlon(start_wp_latlong[1], start_wp_latlong[0])
    end_wp_xy = utm.from_latlon(end_wp_latlong[1], end_wp_latlong[0])
    arc_center_xy = utm.from_latlon(arc_center_latlong[1], arc_center_latlong[0])


    mid_wp_xy = ((start_wp_xy[0] + end_wp_xy[0])/2,(start_wp_xy[1] + end_wp_xy[1])/2)

    arc_radius = np.sqrt(np.square(start_wp_xy[0]-arc_center_xy[0]) + np.square(start_wp_xy[1]-arc_center_xy[1]))

    theta = np.arctan((mid_wp_xy[1]-arc_center_xy[1])/(mid_wp_xy[0]-arc_center_xy[0]))

    if (Turn == 'L'):
        x_comp = -np.cos(theta) * arc_radius + arc_center_xy[0]
        y_comp = -np.sin(theta) * arc_radius + arc_center_xy[1]
    else:
        x_comp = np.cos(theta) * arc_radius + arc_center_xy[0]
        y_comp = np.sin(theta) * arc_radius + arc_center_xy[1]

    sql_query = "DROP TABLE IF EXISTS ZZZ; " \
                "SELECT ST_Transform(ST_SetSRID(ST_MakePoint(" + str(x_comp) + "," + str(y_comp) + "),"\
                + str(UTM_zone)+"),4326) " \
                "INTO ZZZ;"
    print(sql_query)
    cur.execute(sql_query)
    conn.commit()

    sql_query =  "drop table if exists vvv; " + \
                 "SELECT ST_Transform(ST_SetSRID(ST_GeomFromEWKT(" + \
                 "\'CIRCULARSTRING(" + \
                 str(IF_wp_xy[0]) + " " + str(IF_wp_xy[1]) + "," + \
                 str(WP2_wp_xy[0]) + " " + str(WP2_wp_xy[1]) + "," + \
                 str(WP2_wp_xy[0]) + " " + str(WP2_wp_xy[1]) + "," + \
                 str(start_wp_xy[0]) + " " + str(start_wp_xy[1]) + "," + \
                 str(start_wp_xy[0]) + " " + str(start_wp_xy[1])  + "," + \
                 str(x_comp) + " " + str(y_comp) + "," + \
                 str(end_wp_xy[0]) + " " + str(end_wp_xy[1]) + ")\'), " + \
                 str(UTM_zone)+"), 4326) INTO vvv; "

    print(sql_query)
    cur.execute(sql_query)
    conn.commit()