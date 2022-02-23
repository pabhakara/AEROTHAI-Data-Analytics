drop table if exists iap_legs_2201;

select *
into iap_legs_2201
from
(select *
from iap_legs_without_af_or_rf_2201
union
select *
from iap_legs_af_2201
union
select *
from iap_legs_rf_2201) a;

delete
from iap_legs_2201
where st_length(geom) > 300;

delete
from iap_legs_af_2201
where st_length(geom) > 300;

delete
from iap_legs_rf_2201
where st_length(geom) > 300;

delete
from iap_legs_without_af_or_rf_2201
where st_length(geom) > 300;

delete
from sid_legs_2201
where st_length(geom) > 300;

delete
from star_legs_2201
where st_length(geom) > 300;

delete
from airways_2201
where st_length(geom) > 300;

delete
from airways_segments_2201
where st_length(geom) > 300;

DROP TABLE IF EXISTS iap_legs_2201;

SELECT *
INTO iap_legs_2201
FROM
(SELECT * FROM public.iap_legs_without_af_or_rf_2201
union
SELECT * FROM public.iap_legs_rf_2201
union
SELECT * FROM public.iap_legs_af_2201) a;