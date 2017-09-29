
-------- 1 产生中间表
drop table if exists mpintranet.attrvalue;
CREATE TABLE  if not exists mpintranet.attrvalue(
attrvalue  String
)
CLUSTERED BY (attrvalue) INTO 113 BUCKETS
STORED AS ORC
;

insert into table mpintranet.attrvalue
select concat(attrname, '_', attrvalue) from elengjing.women_clothing_item_attr where categoryid = '50000697' group by concat(attrname, '_', attrvalue)
;


drop table if exists mpintranet.itemid;
CREATE TABLE  if not exists mpintranet.itemid(
itemid  BIGINT
)
CLUSTERED BY (itemid) INTO 113 BUCKETS
STORED AS ORC
;

insert into table mpintranet.itemid
select itemid from elengjing.women_clothing_item_attr where categoryid = '50000697' group by itemid
;


drop table if exists mpintranet.item_attr;
CREATE TABLE  if not exists mpintranet.item_attr(
itemid  BIGINT,
attrvalue  String
)
CLUSTERED BY (itemid) INTO 581 BUCKETS
STORED AS ORC
;

insert into table mpintranet.item_attr
select
itemid, concat(attrname, '_', attrvalue) as attrvalue
from
elengjing.women_clothing_item_attr
where categoryid = '50000697'
;


------- 产生全量属性表
drop table if exists mpintranet.itemid_attrvalue;
CREATE TABLE  if not exists mpintranet.itemid_attrvalue(
itemid  BIGINT,
attrvalue  String
)
CLUSTERED BY (itemid) INTO 581 BUCKETS
STORED AS ORC;

insert into table mpintranet.itemid_attrvalue
select
b.itemid,
a.attrvalue
from
mpintranet.attrvalue a
cross join
mpintranet.itemid b
;


---------join 全量表 ，产出结果
drop table if exists mpintranet.itemid_attrvalue_tag;
CREATE TABLE  if not exists mpintranet.itemid_attrvalue_tag(
itemid  BIGINT,
attrvalue  String,
tag  String
)
CLUSTERED BY (itemid) INTO 783 BUCKETS
STORED AS ORC
;

set mapred.reduce.tasks=500;
insert into table mpintranet.itemid_attrvalue_tag
select
a.itemid,
a.attrvalue,
if(b.itemid is null, '0', '1')
from
mpintranet.itemid_attrvalue a
left join
mpintranet.item_attr b
on a.itemid = b.itemid and a.attrvalue = b.attrvalue
;


---------- join 结果表
drop table if exists mpintranet.attrvalue_result;
CREATE TABLE  if not exists mpintranet.attrvalue_result(
itemid  BIGINT,
attrvalue  String,
tag  String
)
CLUSTERED BY (itemid) INTO 113 BUCKETS
STORED AS ORC
;

set mapred.reduce.tasks=-1;
insert into table mpintranet.attrvalue_result

select
t.itemid, group_concat(t.attrvalue, ",") as attrvalue,  group_concat(t.tag, ",") as tag
from
(
SELECT
itemid, attrvalue, tag, split(attrvalue, ',')[0] as attrname
FROM
mpintranet.itemid_attrvalue_tag
DISTRIBUTE BY itemid
SORT BY attrname desc
) t
group by t.itemid
;


------------end-------------


drop table if exists mpintranet.attrvalue_export;
CREATE TABLE  if not exists mpintranet.attrvalue_export(
itemid  BIGINT,
tag  String
)
CLUSTERED BY (itemid) INTO 113 BUCKETS
STORED AS ORC
;


insert into table mpintranet.attrvalue_export
select itemid, tag from mpintranet.attrvalue_result
;


