
--------------------------------------------------------------------------------------------------------------------------------------------------
drop table if exists mpintranet.c_counter_1;
CREATE TABLE  if not exists mpintranet.c_counter_1(
category_id BIGINT,
counter BIGINT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY'\t';



drop table if exists mpintranet.a_counter_1;
CREATE TABLE  if not exists mpintranet.a_counter_1(
attrname STRING,
counter BIGINT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY'\t';


drop table if exists mpintranet.c_a_counter_1;
CREATE TABLE  if not exists mpintranet.c_a_counter_1(
category_id BIGINT,
attrname STRING,
counter BIGINT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY'\t';



insert into table mpintranet.c_counter_1
select
categoryid,count(distinct AttrValue)
from
elengjing.women_clothing_item_attr
-- where categoryid in (1623,1629,162103,162104,162116,162201,162205,162401,162402,162403,162404,162701,162702,162703,50000671,50000697,50000852,50003509,50003510,50003511,50005065,50008897,50008898,50008899,50008900,50008901,50008903,50008904,50008905,50011277,50011411,50011412,50011413,50013196,50026651,121434004,123216004,50007068,50010850,50013194,50022566,121412004)
group by categoryid;


insert into table mpintranet.a_counter_1
select
AttrName,count(distinct AttrValue)
from
elengjing.women_clothing_item_attr
-- where categoryid in (1623,1629,162103,162104,162116,162201,162205,162401,162402,162403,162404,162701,162702,162703,50000671,50000697,50000852,50003509,50003510,50003511,50005065,50008897,50008898,50008899,50008900,50008901,50008903,50008904,50008905,50011277,50011411,50011412,50011413,50013196,50026651,121434004,123216004,50007068,50010850,50013194,50022566,121412004)
group by AttrName;



insert into table mpintranet.c_a_counter_1
select
categoryid,AttrName,count(distinct AttrValue)
from
elengjing.women_clothing_item_attr
-- where categoryid in (1623,1629,162103,162104,162116,162201,162205,162401,162402,162403,162404,162701,162702,162703,50000671,50000697,50000852,50003509,50003510,50003511,50005065,50008897,50008898,50008899,50008900,50008901,50008903,50008904,50008905,50011277,50011411,50011412,50011413,50013196,50026651,121434004,123216004,50007068,50010850,50013194,50022566,121412004)
group by categoryid,AttrName;



------------------------------------------统计taobo属性--------------------------------------------------

drop table if exists mpintranet.c_counter;
CREATE TABLE  if not exists mpintranet.c_counter(
category_id BIGINT,
counter BIGINT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY'\t';

LOAD DATA LOCAL INPATH '/home/data/tmp/c_df.csv' OVERWRITE INTO TABLE mpintranet.c_counter;


drop table if exists mpintranet.a_counter;
CREATE TABLE  if not exists mpintranet.a_counter(
attrname STRING,
counter BIGINT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY'\t';

LOAD DATA LOCAL INPATH '/home/data/tmp/a_df.csv' OVERWRITE INTO TABLE mpintranet.a_counter;


drop table if exists mpintranet.c_a_counter;
CREATE TABLE  if not exists mpintranet.c_a_counter(
category_id BIGINT,
attrname STRING,
counter BIGINT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY'\t';

LOAD DATA LOCAL INPATH '/home/data/tmp/c_a_df.csv' OVERWRITE INTO TABLE mpintranet.c_a_counter;



------------------------------------------统计taobo属性--------------------------------------------------

select
t.attrname,
t.counter as "taobo_counter",
w.counter as "mp_counter"
from
mpintranet.a_counter t
inner join
mpintranet.a_counter_1 w
on t.attrname = w.attrname ;


select
t.category_id,
c.categoryname,
t.counter as "taobo_counter",
w.counter as "mp_counter"
from
mpintranet.c_counter t
inner join
mpintranet.c_counter_1 w
on t.category_id = w.category_id
left join
elengjing.category c
on c.categoryid = t.category_id;



select
t.category_id,
c.categoryname,
t.attrname,
t.counter as "taobo_counter",
w.counter as "mp_counter"
from
mpintranet.c_a_counter t
inner join
mpintranet.c_a_counter_1 w
on t.category_id = w.category_id and t.attrname = w.attrname
left join
elengjing.category c
on c.categoryid = t.category_id;









