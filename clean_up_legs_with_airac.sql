drop table if exists iap_legs_2203;

select *
into iap_legs_2203
from
(select *
from iap_legs_without_af_or_rf_2203
union
select *
from iap_legs_af_2203
union
select *
from iap_legs_rf_2203) a;

delete
from iap_legs_2203
where st_length(geom) > 300;

delete
from iap_legs_af_2203
where st_length(geom) > 300;

delete
from iap_legs_rf_2203
where st_length(geom) > 300;

delete
from iap_legs_without_af_or_rf_2203
where st_length(geom) > 300;

delete
from sid_legs_2203
where st_length(geom) > 300;

delete
from star_legs_2203
where st_length(geom) > 300;

delete
from airways_2203
where st_length(geom) > 300;

delete
from airways_segments_2203
where st_length(geom) > 300;

DROP TABLE IF EXISTS iap_legs_2203;

SELECT *
INTO iap_legs_2203
FROM
(SELECT * FROM public.iap_legs_without_af_or_rf_2203
union
SELECT * FROM public.iap_legs_rf_2203
union
SELECT * FROM public.iap_legs_af_2203) a;