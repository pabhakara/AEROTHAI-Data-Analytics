import os
import json
import psycopg2
from geojson import Point, LineString
from psycopg2.extras import execute_values
from shapely.geometry import shape
from shapely.geometry import LineString as ShapelyLineString
from datetime import datetime
from collections import Counter

# --- Root directory with LogBook-*.json files (including subdirectories) ---
root_directory = "/Users/pongabha/Library/CloudStorage/Dropbox/Workspace/BearCat Aviation/Aireon Data/Mongolia Jan 26- Feb 3/"

# --- PostgreSQL connection ---
conn = psycopg2.connect(
    user="postgres",
    password="password",
    host="127.0.0.1",
    port="5432",
    database="temp",
    options="-c search_path=dbo,aireon,public"
)
cur = conn.cursor()

# --- Drop previous tables if they exist in aireon ---
cur.execute("DROP TABLE IF EXISTS aireon.logbook_features CASCADE;")
cur.execute("DROP TABLE IF EXISTS aireon.logbook_fullpaths CASCADE;")

# --- Create logbook_features with SRID 4326 ---
cur.execute("""
CREATE TABLE IF NOT EXISTS public.logbook_features (
    id SERIAL PRIMARY KEY,
    hop_id TEXT,
    type TEXT,
    event_name TEXT,
    timestamp BIGINT,
    date_time TEXT,
    alt_barometric FLOAT,
    alt_geometric FLOAT,
    speed_ground FLOAT,
    heading FLOAT,
    aid TEXT,
    airport TEXT,
    departure TEXT,
    arrival TEXT,
    distance_km FLOAT,
    cruising_time INT,
    climbing_time INT,
    descending_time INT,
    turning_time INT,
    geom geometry(Geometry, 4326)
);
""")

# --- Create logbook_fullpaths with extended attributes ---
cur.execute("""
CREATE TABLE IF NOT EXISTS public.logbook_fullpaths (
    id SERIAL PRIMARY KEY,
    icao_address TEXT,
    aid TEXT,
    dot DATE,
    hop_id TEXT,
    departure TEXT,
    departure_iata_code TEXT,
    departure_country TEXT,
    departure_municipality TEXT,
    departure_runway TEXT,
    departure_alt FLOAT,
    arrival TEXT,
    arrival_iata_code TEXT,
    arrival_country TEXT,
    arrival_municipality TEXT,
    arrival_runway TEXT,
    arrival_alt FLOAT,
    airborne BOOLEAN,
    distance_km FLOAT,
    cruising_time INT,
    climbing_time INT,
    descending_time INT,
    turning_time INT,
    start_time TEXT,
    stop_time TEXT,
    geom geometry(LineString, 4326)
);
""")
conn.commit()

# --- Prepare data containers ---
feature_data = []
fullpath_data = []

# --- Walk through subdirectories and collect LogBook-*.json files ---
for dirpath, dirnames, filenames in os.walk(root_directory):
    for filename in filenames:
        if filename.startswith("LogBook-") and filename.endswith(".json"):
            filepath = os.path.join(dirpath, filename)
            print(f"\U0001F4E6 Processing: {filepath}")

            with open(filepath, 'r') as f:
                logbook_data = json.load(f)

            for flight in logbook_data.get('flights', []):
                icao = flight.get("icao_address")

                for hop in flight.get('hops', []):
                    hop_id = hop.get("hop_id")
                    coords = []
                    events = hop.get("events", [])

                    # ✅ Extract AID using majority voting from events
                    aid_counts = Counter(e.get("aid") for e in events if e.get("aid"))
                    aid = aid_counts.most_common(1)[0][0] if aid_counts else None

                    # ✅ Extract DOT as proper DATE from start_date_time
                    dot_str = hop.get("start_date_time", "").split(" ")[0]
                    dot = datetime.strptime(dot_str, "%Y-%m-%d").date() if dot_str else None

                    for event in events:
                        lat = event.get('lat')
                        lng = event.get('lng')
                        if lat is not None and lng is not None:
                            coords.append((lng, lat))
                            point_geom = Point((lng, lat))

                            feature_data.append((
                                hop_id,
                                "event",
                                event.get("name"),
                                event.get("timestamp"),
                                event.get("date_time"),
                                event.get("alt_barometric"),
                                event.get("alt_geometric"),
                                event.get("speed_ground"),
                                event.get("heading"),
                                event.get("aid"),
                                event.get("airport"),
                                None, None, None, None, None, None, None,
                                shape(point_geom).wkb
                            ))

                    if len(coords) >= 2:
                        hop_line = LineString(coords)

                        feature_data.append((
                            hop_id,
                            "flight_path",
                            None, None, None,
                            None, None, None, None,
                            None, None,
                            hop.get("departure"),
                            hop.get("arrival"),
                            hop.get("distance"),
                            hop.get("cruising_time"),
                            hop.get("climbing_time"),
                            hop.get("descending_time"),
                            hop.get("turning_time"),
                            shape(hop_line).wkb
                        ))

                        fullpath_data.append((
                            icao,
                            aid,
                            dot,
                            hop_id,
                            hop.get("departure"),
                            hop.get("departure_iata_code"),
                            hop.get("departure_country"),
                            hop.get("departure_municipality"),
                            hop.get("departure_runway"),
                            hop.get("departure_alt"),
                            hop.get("arrival"),
                            hop.get("arrival_iata_code"),
                            hop.get("arrival_country"),
                            hop.get("arrival_municipality"),
                            hop.get("arrival_runway"),
                            hop.get("arrival_alt"),
                            hop.get("airborne"),
                            hop.get("distance"),
                            hop.get("cruising_time"),
                            hop.get("climbing_time"),
                            hop.get("descending_time"),
                            hop.get("turning_time"),
                            hop.get("start_date_time"),
                            hop.get("stop_date_time"),
                            shape(hop_line).wkb
                        ))

# --- Insert into logbook_features ---
if feature_data:
    execute_values(cur, """
        INSERT INTO public.logbook_features (
            hop_id, type, event_name, timestamp, date_time,
            alt_barometric, alt_geometric, speed_ground, heading,
            aid, airport, departure, arrival,
            distance_km, cruising_time, climbing_time, descending_time, turning_time,
            geom
        ) VALUES %s
    """, feature_data, page_size=100)

# --- Insert into logbook_fullpaths ---
if fullpath_data:
    execute_values(cur, """
        INSERT INTO public.logbook_fullpaths (
            icao_address, aid, dot, hop_id,
            departure, departure_iata_code, departure_country, departure_municipality, departure_runway, departure_alt,
            arrival, arrival_iata_code, arrival_country, arrival_municipality, arrival_runway, arrival_alt,
            airborne, distance_km, cruising_time, climbing_time, descending_time, turning_time,
            start_time, stop_time, geom
        ) VALUES %s
    """, fullpath_data, page_size=50)

# --- Move tables to aireon schema ---
cur.execute("ALTER TABLE public.logbook_features SET SCHEMA aireon;")
cur.execute("ALTER TABLE public.logbook_fullpaths SET SCHEMA aireon;")

# --- Create spatial indexes ---
cur.execute("CREATE INDEX IF NOT EXISTS logbook_features_geom_idx ON aireon.logbook_features USING GIST (geom);")
cur.execute("CREATE INDEX IF NOT EXISTS logbook_fullpaths_geom_idx ON aireon.logbook_fullpaths USING GIST (geom);")

conn.commit()
cur.close()
conn.close()

print("✅ All logbooks processed. Full hop details loaded, AID (majority), DOT (date) extracted, tables moved to 'aireon', spatial indexes created.")
