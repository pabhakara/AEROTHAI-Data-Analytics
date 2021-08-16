import pandas_access as mdb
import psycopg2
from sqlalchemy import create_engine

pgdb = 'airac_2021_08_astnav'
pguser = 'postgres'
pgpswd = 'password'
pghost = 'localhost'
pgport = '5432'

db_filename = '/Users/pongabha/Dropbox/Workspace/AEROTHAI Data Analytics/NavData/airsimtech_native_2108/ASTNAV.mdb'

conpg = psycopg2.connect(database=pgdb, user=pguser, password=pgpswd,
                                 host=pghost, port=pgport)

engine = create_engine('postgresql://'+pguser+':'+pgpswd+'@'+pghost+':'+pgport+'/'+pgdb)

for table in mdb.list_tables(db_filename):
  if (table == "TerminalLegs"):
    df = mdb.read_table(db_filename, table,dtype={'IDWaypoint': 'float'})
  else:
    df = mdb.read_table(db_filename, table)
  df.to_sql(table, engine,if_exists ='replace')
  print('Create table ' + table)

# curpg = conpg.cursor()
#
# sql_text = 'drop table if exists \"airport_geom\";' + \
# 'select *,' + \
# 'ST_SetSRID(ST_MakePoint(CAST(replace(\"Long\"::text, \',\', \'.\') AS float),CAST (replace(\"Lat\"::text, \',\', \'.\') AS float)),4326) AS geom ' + \
# 'into \"airport_geom\" ' + \
# 'from \"Airport\"; ' + \
# 'drop table if exists \"navaid_geom\"; ' + \
# 'select *, ' + \
# 'ST_SetSRID(ST_MakePoint(CAST(replace(\"Long\"::text, \',\', \'.\') AS float),CAST (replace(\"Lat\"::text, \',\', \'.\') AS float)),4326) AS geom ' + \
# 'into \"navaid_geom\" ' + \
# 'from \"Navaid\"; ' + \
# 'drop table if exists \"runway_geom\"; ' + \
# 'select *, ' + \
# 'ST_SetSRID(ST_MakePoint(CAST(replace(\"LongBegin\"::text, \',\', \'.\') AS float),CAST (replace(\"LatBegin\"::text, \',\', \'.\') AS float)),4326) AS begin_point, ' + \
# 'ST_SetSRID(ST_MakePoint(CAST(replace(\"LongEnd\"::text, \',\', \'.\') AS float),CAST (replace(\"LatEnd\"::text, \',\', \'.\') AS float)),4326) AS end_point, ' + \
# 'ST_MakeLine(ST_SetSRID(ST_MakePoint(CAST(replace(\"LongBegin\"::text, \',\', \'.\') AS float),CAST (replace(\"LatBegin\"::text, \',\', \'.\') AS float)),4326),ST_SetSRID(ST_MakePoint(CAST(replace(\"LongEnd\"::text, \',\', \'.\') AS float),CAST (replace(\"LatEnd\"::text, \',\', \'.\') AS float)),4326)) AS geom ' + \
# 'into \"runway_geom\" ' + \
# 'from \"Runway\"; ' + \
# 'drop table if exists \"waypoint_geom\"; ' + \
# 'select *, ' + \
# 'ST_SetSRID(ST_MakePoint(CAST(replace(\"Long\"::text, \',\', \'.\') AS float),CAST (replace(\"Lat\"::text, \',\', \'.\') AS float)),4326) AS geom ' + \
# 'into \"waypoint_geom\" ' + \
# 'from \"Waypoint\"; ' + \
# 'drop table if exists terminallegs_temp; ' + \
# 'select a.\"ID\" as airport_id, a.\"Ident\" as airport_ident, ' + \
# 't.\"Procedure\" as procedure_type, t.\"Name\" as procedure_ident, ' + \
# 't.\"IDRunway\" as rwy_id, ' + \
# 'wp.\"Ident\" as waypoint_ident, ' + \
# 'wp.geom as wp_geom, ' + \
# 'tl.* ' + \
# 'into terminallegs_temp ' + \
# 'from \"Terminals\" t, \"Airport\" a, \"TerminalLegs\" tl, \"waypoint_geom\" wp ' + \
# 'where (t.\"IDAirport\" = a.\"ID\") ' + \
# 'and (tl.\"IDTerminal\" = t.\"ID\") ' + \
# 'and (wp.\"ID\" = tl.\"IDWaypoint\") ' + \
# 'order by tl.\"ID\" ASC; ' + \
# 'ALTER TABLE terminallegs_temp ' + \
# 'ADD COLUMN rwy_designator character varying; ' + \
# 'UPDATE terminallegs_temp ' + \
# 'SET rwy_designator=(SELECT \"Designator\" ' + \
# 'FROM \"Runway\" ' + \
# 'WHERE \"Runway\".\"ID\"=terminallegs_temp.rwy_id); '
#
# curpg.execute(sql_text)
#
# conpg.commit()