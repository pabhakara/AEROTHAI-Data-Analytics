

directory = "/Users/pongabha/Library/CloudStorage/Dropbox/Workspace/BearCat Aviation/Aireon Data/Mongolia Jan 26- Feb 3/2025-01/"
filename = "LogBook-20250126_100.json"

import json
import geojson
from geojson import Feature, FeatureCollection, Point, LineString

# Load the logbook JSON file
with open(f"{directory}{filename}", 'r') as f:
    logbook_data = json.load(f)

features = []

# Loop through each flight
for flight in logbook_data.get('flights', []):
    for hop in flight.get('hops', []):
        hop_id = hop.get("hop_id")
        coordinates = []

        # Add Point features for events
        for event in hop.get('events', []):
            lat = event.get('lat')
            lng = event.get('lng')

            if lat is not None and lng is not None:
                coordinates.append((lng, lat))

                # Create Point feature
                point = Point((lng, lat))
                properties = {
                    "type": "event",
                    "hop_id": hop_id,
                    "event_name": event.get("name"),
                    "timestamp": event.get("timestamp"),
                    "date_time": event.get("date_time"),
                    "alt_barometric": event.get("alt_barometric"),
                    "alt_geometric": event.get("alt_geometric"),
                    "speed_ground": event.get("speed_ground"),
                    "heading": event.get("heading"),
                    "aid": event.get("aid"),
                    "airport": event.get("airport"),
                }
                features.append(Feature(geometry=point, properties=properties))

        # Add LineString feature for the flight path
        if len(coordinates) >= 2:
            line_properties = {
                "type": "flight_path",
                "hop_id": hop_id,
                "departure": hop.get("departure"),
                "departure_iata": hop.get("departure_iata_code"),
                "departure_time": hop.get("start_date_time"),
                "arrival": hop.get("arrival"),
                "arrival_iata": hop.get("arrival_iata_code"),
                "arrival_time": hop.get("stop_date_time"),
                "distance_km": hop.get("distance"),
                "airborne": hop.get("airborne"),
                "cruising_time": hop.get("cruising_time"),
                "climbing_time": hop.get("climbing_time"),
                "descending_time": hop.get("descending_time"),
                "turning_time": hop.get("turning_time"),
            }

            line = LineString(coordinates)
            features.append(Feature(geometry=line, properties=line_properties))

# Create FeatureCollection and write GeoJSON
geojson_data = FeatureCollection(features)

with open(f"{directory}logbook_combined.geojson", 'w') as f:
    geojson.dump(geojson_data, f, indent=2)

print("✅ GeoJSON file created: logbook_combined.geojson (Points + Lines)")
