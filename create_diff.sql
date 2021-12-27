drop table if exists "airports_2110";
select *,
ST_SetSRID(ST_MakePoint(airport_ref_longitude,airport_ref_latitude),4326) AS geom
into "airports_2110"
from "tbl_airports";

drop table if exists "enroute_ndb_2110";
select *,
ST_SetSRID(ST_MakePoint(ndb_longitude,ndb_latitude),4326) AS geom
into "enroute_ndb_2110"
from "tbl_enroute_ndbnavaids";

drop table if exists "enroute_waypoints_2110";
select *,
ST_SetSRID(ST_MakePoint(waypoint_longitude,waypoint_latitude),4326) AS geom
into "enroute_waypoints_2110"
from "tbl_enroute_waypoints";

drop table if exists "gate_2110";
select *,
ST_SetSRID(ST_MakePoint(gate_longitude,gate_latitude),4326) AS geom
into "gate_2110"
from "tbl_gate";

drop table if exists "gls_2110";
select *,
ST_SetSRID(ST_MakePoint(station_longitude,station_latitude),4326) AS geom
into "gls_2110"
from "tbl_gls";

drop table if exists "holdings_2110";
select *,
ST_SetSRID(ST_MakePoint(waypoint_longitude,waypoint_latitude),4326) AS geom
into "holdings_2110"
from "tbl_holdings";

drop table if exists "localizer_marker_2110";
select *,
ST_SetSRID(ST_MakePoint(marker_longitude,marker_latitude),4326) AS geom
into "localizer_marker_2110"
from "tbl_localizer_marker";

drop table if exists "localizers_2110";
select *,
ST_SetSRID(ST_MakePoint(llz_longitude,llz_latitude),4326) AS geom
into "localizers_2110"
from "tbl_localizers_glideslopes";

drop table if exists "glideslopes_2110";
select *,
ST_SetSRID(ST_MakePoint(gs_longitude,gs_latitude),4326) AS geom
into "glideslopes_2110"
from "tbl_localizers_glideslopes";


drop table if exists "runways_2110";
select *,
ST_SetSRID(ST_MakePoint(runway_longitude,runway_latitude),4326) AS geom
into "runways_2110"
from "tbl_runways";


drop table if exists "sids_wp_2110";
select *,
ST_SetSRID(ST_MakePoint(waypoint_longitude,waypoint_latitude),4326) AS geom
into "sids_wp_2110"
from "tbl_sids";

drop table if exists "stars_wp_2110";
select *,
ST_SetSRID(ST_MakePoint(waypoint_longitude,waypoint_latitude),4326) AS geom
into "stars_wp_2110"
from "tbl_stars";

drop table if exists "iaps_wp_2110";
select *,
ST_SetSRID(ST_MakePoint(waypoint_longitude,waypoint_latitude),4326) AS geom
into "iaps_wp_2110"
from "tbl_iaps";

drop table if exists "terminal_ndb_2110";
select *,
ST_SetSRID(ST_MakePoint(ndb_longitude,ndb_latitude),4326) AS geom
into "terminal_ndb_2110"
from "tbl_terminal_ndbnavaids";

drop table if exists "terminal_waypoints_2110";
select *,
ST_SetSRID(ST_MakePoint(waypoint_longitude,waypoint_latitude),4326) AS geom
into "terminal_waypoints_2110"
from "tbl_terminal_waypoints";

drop table if exists "vor_2110";
select *,
ST_SetSRID(ST_MakePoint(vor_longitude,vor_latitude),4326) AS geom
into "vor_2110"
from "tbl_vhfnavaids";

drop table if exists "dme_2110";
select *,
ST_SetSRID(ST_MakePoint(dme_longitude,dme_latitude),4326) AS geom
into "dme_2110"
from "tbl_vhfnavaids";

drop table if exists "airways_wp_2110";
select *,
ST_SetSRID(ST_MakePoint(waypoint_longitude,waypoint_latitude),4326) AS geom
into "airways_wp_2110"
from "tbl_enroute_airways";