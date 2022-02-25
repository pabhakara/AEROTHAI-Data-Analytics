drop table if exists "airports";
select *,
ST_SetSRID(ST_MakePoint(airport_ref_longitude,airport_ref_latitude),4326) AS geom
into "airports"
from "tbl_airports";

drop table if exists "enroute_ndb";
select *,
ST_SetSRID(ST_MakePoint(ndb_longitude,ndb_latitude),4326) AS geom
into "enroute_ndb"
from "tbl_enroute_ndbnavaids";

drop table if exists "enroute_waypoints";
select *,
ST_SetSRID(ST_MakePoint(waypoint_longitude,waypoint_latitude),4326) AS geom
into "enroute_waypoints"
from "tbl_enroute_waypoints";

drop table if exists "gate";
select *,
ST_SetSRID(ST_MakePoint(gate_longitude,gate_latitude),4326) AS geom
into "gate"
from "tbl_gate";

drop table if exists "gls";
select *,
ST_SetSRID(ST_MakePoint(station_longitude,station_latitude),4326) AS geom
into "gls"
from "tbl_gls";

drop table if exists "holdings";
select *,
ST_SetSRID(ST_MakePoint(waypoint_longitude,waypoint_latitude),4326) AS geom
into "holdings"
from "tbl_holdings";

drop table if exists "localizer_marker";
select *,
ST_SetSRID(ST_MakePoint(marker_longitude,marker_latitude),4326) AS geom
into "localizer_marker"
from "tbl_localizer_marker";

drop table if exists "localizers";
select *,
ST_SetSRID(ST_MakePoint(llz_longitude,llz_latitude),4326) AS geom
into "localizers"
from "tbl_localizers_glideslopes";

drop table if exists "glideslopes";
select *,
ST_SetSRID(ST_MakePoint(gs_longitude,gs_latitude),4326) AS geom
into "glideslopes"
from "tbl_localizers_glideslopes";


drop table if exists "runways";
select *,
ST_SetSRID(ST_MakePoint(runway_longitude,runway_latitude),4326) AS geom
into "runways"
from "tbl_runways";


drop table if exists "sids_wp";
select *,
ST_SetSRID(ST_MakePoint(waypoint_longitude,waypoint_latitude),4326) AS geom
into "sids_wp"
from "tbl_sids";

drop table if exists "stars_wp";
select *,
ST_SetSRID(ST_MakePoint(waypoint_longitude,waypoint_latitude),4326) AS geom
into "stars_wp"
from "tbl_stars";

drop table if exists "iaps_wp";
select *,
ST_SetSRID(ST_MakePoint(waypoint_longitude,waypoint_latitude),4326) AS geom
into "iaps_wp"
from "tbl_iaps";

drop table if exists "terminal_ndb";
select *,
ST_SetSRID(ST_MakePoint(ndb_longitude,ndb_latitude),4326) AS geom
into "terminal_ndb"
from "tbl_terminal_ndbnavaids";

drop table if exists "terminal_waypoints";
select *,
ST_SetSRID(ST_MakePoint(waypoint_longitude,waypoint_latitude),4326) AS geom
into "terminal_waypoints"
from "tbl_terminal_waypoints";

drop table if exists "vor";
select *,
ST_SetSRID(ST_MakePoint(vor_longitude,vor_latitude),4326) AS geom
into "vor"
from "tbl_vhfnavaids"
where navaid_class like '%V%';

drop table if exists "dme";
select *,
ST_SetSRID(ST_MakePoint(dme_longitude,dme_latitude),4326) AS geom
into "dme"
from "tbl_vhfnavaids";

drop table if exists "airways_wp";
select *,
ST_SetSRID(ST_MakePoint(waypoint_longitude,waypoint_latitude),4326) AS geom
into "airways_wp"
from "tbl_enroute_airways";