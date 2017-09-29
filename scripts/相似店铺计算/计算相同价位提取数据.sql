
drop table if exists mpintranet.attr_value_text;
CREATE TABLE  if not exists mpintranet.attr_value_text(
categoryid String,
attr String
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY'\t'
;

LOAD DATA LOCAL INPATH '/home/data/tmp/attr_tag_name_value_list.pyattr_tag_name_value_list.py' OVERWRITE INTO TABLE mpintranet.attr_value_text
;



drop table if exists mpintranet.attr_value;
create table mpintranet.attr_value 
STORED AS ORC
as 
select * from 
mpintranet.attr_value_text
where
categoryid = 50000671
;



drop table if exists mpintranet.attr_uniquit_itemid;
CREATE TABLE  if not exists mpintranet.attr_uniquit_itemid(
itemid bigint
)
CLUSTERED BY (itemid) INTO 997 BUCKETS
STORED AS ORC
;

insert into table mpintranet.attr_uniquit_itemid
select itemid from elengjing.women_clothing_item_attr
where
categoryid = 50000671 and attrname != '品牌'
group by itemid
;





drop table if exists mpintranet.item_attr;
CREATE TABLE  if not exists mpintranet.item_attr(
itemid bigint,
attr string
)
CLUSTERED BY (itemid) INTO 997 BUCKETS
STORED AS ORC
;

insert into table mpintranet.item_attr
select itemid, concat(attrname, ":", "attrvalue") from
elengjing.women_clothing_item_attr
where
categoryid = 50000671 and attrname != '品牌'
group by itemid, concat(attrname, ":", "attrvalue")
;





drop table if exists mpintranet.attr_itemid_value;
CREATE TABLE  if not exists mpintranet.attr_itemid_value(
itemid bigint,
attr String
)
CLUSTERED BY (itemid) INTO 997 BUCKETS
STORED AS ORC
;


insert into table mpintranet.attr_itemid_value
select 
t1.itemid,
t2.attr
from 
mpintranet.attr_uniquit_itemid t1
cross join
mpintranet.attr_value t2
;



drop table if exists mpintranet.attr_itemid_value_data;
CREATE TABLE  if not exists mpintranet.attr_itemid_value_data(
itemid bigint,
attr String,
data int
)
CLUSTERED BY (itemid) INTO 997 BUCKETS
STORED AS ORC
;


set mapred.reduce.tasks = 1000;

insert into table mpintranet.attr_itemid_value_data
select 
t1.*,
if(nvl(t2.itemid, 0) = 0, 0, 1)
from 
mpintranet.attr_itemid_value t1
left join
mpintranet.item_attr t2
on t1.itemid = t2.itemid and t1.attr = t2.attr
;



drop table if exists mpintranet.attr_itemid_value_juzhen;
CREATE TABLE  if not exists mpintranet.attr_itemid_value_juzhen(
itemid bigint,
attr String,
data String
)
CLUSTERED BY (itemid) INTO 997 BUCKETS
STORED AS ORC
;


--- 方法一 

insert into table mpintranet.attr_itemid_value_juzhen
select 
itemid,
group_concat(attr, ",") as attr,
group_concat(cast(data as String), ",") as data
from
(
select 
itemid,
attr,
data,
row_number() over (partition by itemid order by attr desc ) rank
from
mpintranet.attr_itemid_value_data
)
group by itemid
;

--- 方法二
insert into table mpintranet.attr_itemid_value_juzhen

select 
itemid,
group_concat(attr, ",") as attr,
group_concat(cast(data as String), ",") as data
from
(
select 
itemid,
attr,
data
from
mpintranet.attr_itemid_value_data
DISTRIBUTE BY itemid
SORT BY attr desc
)
group by itemid
;


insert overwrite local directory "/home/data/tmp/test_data" 
row format delimited 
fields terminated by ","
select itemid, data from mpintranet.attr_itemid_value_juzhen
;


