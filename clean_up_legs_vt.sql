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

DROP TABLE IF EXISTS star_legs;
SELECT *
INTO star_legs
FROM
(SELECT * FROM star_legs_without_rf
union
SELECT * FROM star_legs_rf) a;

DROP TABLE IF EXISTS sid_legs;
SELECT *
INTO sid_legs
FROM
(SELECT * FROM sid_legs_without_rf
union
SELECT * FROM sid_legs_rf) a;

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

DROP TABLE IF EXISTS iap_legs_without_af_or_rf;
DROP TABLE IF EXISTS iap_legs_af;
DROP TABLE IF EXISTS iap_legs_rf;

DROP TABLE IF EXISTS sbas_path;

do
$$
declare
   stmt text;
   table_rec record;
begin
   for table_rec in (SELECT c.relname as tname
                     FROM pg_catalog.pg_class c
                       LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
                     WHERE c.relkind IN ('r','')
                       AND n.nspname LIKE 'public'
                       AND c.relname like 'tbl%')
   loop
     execute 'DROP TABLE IF EXISTS public.'||table_rec.tname||' cascade';
   end loop;
end;
$$;

DO
$$
DECLARE
    row record;
BEGIN
    FOR row IN SELECT tablename FROM pg_tables WHERE schemaname = 'public' and NOT(tablename like 'spat%')
    LOOP
        EXECUTE 'DROP TABLE IF EXISTS airac_current_vt.' || quote_ident(row.tablename) || ' ;';
        EXECUTE 'ALTER TABLE public.' || quote_ident(row.tablename) || ' SET SCHEMA airac_current_vt;';
    END LOOP;
END;
$$;









