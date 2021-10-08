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