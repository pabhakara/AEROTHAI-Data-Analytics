drop table if exists iap_legs;

select *
into iap_legs
from
(select *
from iap_legs_without_af_or_rf
union
select *
from iap_legs_af
union
select *
from iap_legs_rf) a;

delete
from iap_legs
where st_length(geom) > 300;

delete
from iap_legs_af
where st_length(geom) > 300;

delete
from iap_legs_rf
where st_length(geom) > 300;

delete
from iap_legs_without_af_or_rf
where st_length(geom) > 300;

delete
from sid_legs
where st_length(geom) > 300;

delete
from star_legs
where st_length(geom) > 300;

delete
from airways
where st_length(geom) > 300;

delete
from airways_segments
where st_length(geom) > 300;

DROP TABLE IF EXISTS iap_legs;

SELECT *
INTO iap_legs
FROM
(SELECT * FROM public.iap_legs_without_af_or_rf
union
SELECT * FROM public.iap_legs_rf
union
SELECT * FROM public.iap_legs_af) a;

drop table if exists inbound_distance;
select distinct inbound_distance,geom
into inbound_distance
from airways_segments;

drop table if exists inbound_course;
select distinct inbound_course,geom
into inbound_course
from airways_segments;


drop table if exists minimum_altitude;
select distinct minimum_altitude1,geom
into minimum_altitude
from airways_segments;

delete
from sid_legs_rf
where st_length(geom) > 300;

delete
from sid_legs_without_rf
where st_length(geom) > 300;

delete
from star_legs_rf
where st_length(geom) > 300;

delete
from star_legs_without_rf
where st_length(geom) > 300;