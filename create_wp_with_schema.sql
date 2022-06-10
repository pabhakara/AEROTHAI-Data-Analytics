
drop table if exists current_airac."airports";
select *,
public.ST_SetSRID(postgis.ST_MakePoint(airport_ref_longitude,airport_ref_latitude),4326) AS geom
into current_airac."airports"
from current_airac."tbl_airports";

drop table if exists current_airac."enroute_ndb";
select *,
public.ST_SetSRID(public.ST_MakePoint(ndb_longitude,ndb_latitude),4326) AS geom
into current_airac."enroute_ndb"
from current_airac."tbl_enroute_ndbnavaids";

drop table if exists current_airac."enroute_waypoints";
select *,
public.ST_SetSRID(public.ST_MakePoint(waypoint_longitude,waypoint_latitude),4326) AS geom
into current_airac."enroute_waypoints"
from current_airac."tbl_enroute_waypoints";

drop table if exists current_airac."gate";
select *,
public.ST_SetSRID(public.ST_MakePoint(gate_longitude,gate_latitude),4326) AS geom
into current_airac."gate"
from current_airac."tbl_gate";

drop table if exists current_airac."gls";
select *,
public.ST_SetSRID(public.ST_MakePoint(station_longitude,station_latitude),4326) AS geom
into current_airac."gls"
from current_airac."tbl_gls";

drop table if exists current_airac."holdings";
select *,
public.ST_SetSRID(public.ST_MakePoint(waypoint_longitude,waypoint_latitude),4326) AS geom
into current_airac."holdings"
from current_airac."tbl_holdings";

drop table if exists current_airac."localizer_marker";
select *,
public.ST_SetSRID(public.ST_MakePoint(marker_longitude,marker_latitude),4326) AS geom
into current_airac."localizer_marker"
from current_airac."tbl_localizer_marker";

drop table if exists current_airac."localizers";
select *,
public.ST_SetSRID(public.ST_MakePoint(llz_longitude,llz_latitude),4326) AS geom
into current_airac."localizers"
from current_airac."tbl_localizers_glideslopes";

drop table if exists current_airac."glideslopes";
select *,
public.ST_SetSRID(public.ST_MakePoint(gs_longitude,gs_latitude),4326) AS geom
into current_airac."glideslopes"
from current_airac."tbl_localizers_glideslopes";


drop table if exists current_airac."runways";
select *,
public.ST_SetSRID(public.ST_MakePoint(runway_longitude,runway_latitude),4326) AS geom
into current_airac."runways"
from current_airac."tbl_runways";

drop table if exists "sids_wp";
select *,
public.ST_SetSRID(public.ST_MakePoint(waypoint_longitude,waypoint_latitude),4326) AS geom
into current_airac."sids_wp"
from current_airac."tbl_sids";

drop table if exists "sids_wp_without_rf";
select *,
public.ST_SetSRID(public.ST_MakePoint(waypoint_longitude,waypoint_latitude),4326) AS geom
into current_airac."sids_wp_without_rf"
from current_airac.tbl_sids
                        WHERE airport_identifier like '%'
                        and not(waypoint_identifier is null)
                        and NOT(concat(airport_identifier,procedure_identifier,transition_identifier) in
                        (SELECT distinct concat(airport_identifier,procedure_identifier,transition_identifier) from
                        tbl_sids
                        WHERE path_termination = 'RF'));

drop table if exists "sids_wp_with_rf";
select *,
public.ST_SetSRID(public.ST_MakePoint(waypoint_longitude,waypoint_latitude),4326) AS geom
into "sids_wp_with_rf"
from tbl_sids
                        WHERE airport_identifier like '%'
                        and not(waypoint_identifier is null)
                        and (concat(airport_identifier,procedure_identifier,transition_identifier) in
                        (SELECT distinct concat(airport_identifier,procedure_identifier,transition_identifier) from
                        tbl_sids
                        WHERE path_termination = 'RF'));

drop table if exists "stars_wp";
select *,
public.ST_SetSRID(public.ST_MakePoint(waypoint_longitude,waypoint_latitude),4326) AS geom
into "stars_wp"
from "tbl_stars";

drop table if exists "stars_wp_without_rf";
select *,
public.ST_SetSRID(public.ST_MakePoint(waypoint_longitude,waypoint_latitude),4326) AS geom
into "stars_wp_without_rf"
from tbl_stars
                        WHERE airport_identifier like '%'
                        and not(waypoint_identifier is null)
                        and NOT(concat(airport_identifier,procedure_identifier,transition_identifier) in
                        (SELECT distinct concat(airport_identifier,procedure_identifier,transition_identifier) from
                        tbl_stars
                        WHERE path_termination = 'RF'));

drop table if exists "stars_wp_with_rf";
select *,
public.ST_SetSRID(public.ST_MakePoint(waypoint_longitude,waypoint_latitude),4326) AS geom
into "stars_wp_with_rf"
from tbl_stars
                        WHERE airport_identifier like '%'
                        and not(waypoint_identifier is null)
                        and (concat(airport_identifier,procedure_identifier,transition_identifier) in
                        (SELECT distinct concat(airport_identifier,procedure_identifier,transition_identifier) from
                        tbl_stars
                        WHERE path_termination = 'RF'));

drop table if exists "iaps_wp";
select *,
public.ST_SetSRID(public.ST_MakePoint(waypoint_longitude,waypoint_latitude),4326) AS geom
into "iaps_wp"
from "tbl_iaps";

drop table if exists "terminal_ndb";
select *,
public.ST_SetSRID(public.ST_MakePoint(ndb_longitude,ndb_latitude),4326) AS geom
into "terminal_ndb"
from "tbl_terminal_ndbnavaids";

drop table if exists "terminal_waypoints";
select *,
public.ST_SetSRID(public.ST_MakePoint(waypoint_longitude,waypoint_latitude),4326) AS geom
into "terminal_waypoints"
from "tbl_terminal_waypoints";

drop table if exists "vor";
select *,
public.ST_SetSRID(public.ST_MakePoint(vor_longitude,vor_latitude),4326) AS geom
into "vor"
from "tbl_vhfnavaids"
where navaid_class like '%V%';

drop table if exists "dme";
select *,
public.ST_SetSRID(public.ST_MakePoint(dme_longitude,dme_latitude),4326) AS geom
into "dme"
from "tbl_vhfnavaids";

drop table if exists "airways_wp";
select *,
public.ST_SetSRID(public.ST_MakePoint(waypoint_longitude,waypoint_latitude),4326) AS geom
into "airways_wp"
from "tbl_enroute_airways";

drop table if exists localizer_marker_new;

SELECT a.*,b.llz_bearing,b.station_declination
into localizer_marker_new
FROM localizer_marker a, localizers b
where a.airport_identifier = b.airport_identifier and
a.runway_identifier = b.runway_identifier and
a.llz_identifier = b.llz_identifier;

drop table if exists localizer_marker;

alter table localizer_marker_new
rename to localizer_marker;

drop table if exists airways_wp_distinct;
SELECT distinct waypoint_identifier, waypoint_description_code, geom
INTO airways_wp_distinct
FROM airways_wp;

drop table if exists airways_reporting_point;
SELECT *
into airways_reporting_point
FROM airways_wp_distinct
where not "waypoint_description_code" like '%V%'
and not "waypoint_description_code" like '%L%'
and not "waypoint_description_code" like '%R%';

drop table if exists airways_reporting_point_distinct;
SELECT distinct waypoint_identifier,geom
into airways_reporting_point_distinct
FROM airways_reporting_point;

drop table if exists airways_reporting_point;
alter table airways_reporting_point_distinct
rename to airways_reporting_point;

drop table if exists airways_waypoint;
SELECT *
into airways_waypoint
FROM airways_wp_distinct
where "waypoint_description_code" like '%R%';

drop table if exists airways_waypoint_distinct;
SELECT distinct waypoint_identifier,geom
into airways_waypoint_distinct
FROM airways_waypoint;

drop table if exists airways_waypoint;
alter table airways_waypoint_distinct
rename to airways_waypoint;

drop table if exists airways_vor;
SELECT *
into airways_vor
FROM airways_wp_distinct
where "waypoint_description_code" like '%V%';

drop table if exists airways_vor_distinct;
SELECT distinct waypoint_identifier,geom
into airways_vor_distinct
FROM airways_vor;

drop table if exists airways_vor;
alter table airways_vor_distinct
rename to airways_vor;

drop table if exists "sbas_path";
select *,
public.ST_SetSRID(public.ST_MakePoint(landing_threshold_longitude,landing_threshold_latitude),4326) AS landing_threshold,
public.ST_SetSRID(public.ST_MakePoint(flightpath_alignment_longitude,flightpath_alignment_latitude),4326) AS flightpath_alignment,
public.ST_SetSRID(public.ST_MakeLine(
public.ST_MakePoint(landing_threshold_longitude,landing_threshold_latitude),
public.ST_MakePoint(flightpath_alignment_longitude,flightpath_alignment_latitude))
,4326) AS flightpath,
public.ST_Azimuth(
public.ST_SetSRID(public.ST_MakePoint(landing_threshold_longitude,landing_threshold_latitude),4326)::geography,
public.ST_SetSRID(public.ST_MakePoint(flightpath_alignment_longitude,flightpath_alignment_latitude),4326)::geography
)*180/pi()
AS azimuth
into "sbas_path"
from "tbl_pathpoints";