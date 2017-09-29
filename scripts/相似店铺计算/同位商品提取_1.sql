
-------- 1 产生中间表
drop table if exists mpintranet.attrname;
CREATE TABLE  if not exists mpintranet.attrname(
attrname  String
)
CLUSTERED BY (attrname) INTO 113 BUCKETS
STORED AS ORC
;

insert into table mpintranet.attrname
select attrname from elengjing.women_clothing_item_attr where categoryid = '50000697' group by attrname
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
attrname  String,
attrvalue  String
)
CLUSTERED BY (itemid) INTO 581 BUCKETS
STORED AS ORC;

insert into table mpintranet.item_attr
select
t.itemid, t.attrname, group_concat(t.attrvalue, ":") as attrvalue
from
(
    SELECT
    itemid, attrname, attrvalue
    FROM
    elengjing.women_clothing_item_attr where categoryid = '50000697'
    DISTRIBUTE BY itemid, attrname
    SORT BY attrvalue desc
) t
group by t.itemid, t.attrname
;


------- 产生全量属性表
drop table if exists mpintranet.itemid_attrname;
CREATE TABLE  if not exists mpintranet.itemid_attrname(
itemid  BIGINT,
attrname  String
)
CLUSTERED BY (itemid) INTO 581 BUCKETS
STORED AS ORC;

insert into table mpintranet.itemid_attrname
select
b.itemid,
a.attrname
from
mpintranet.attrname a
cross join
mpintranet.itemid b
;


---------join 全量表 ，产出结果
drop table if exists mpintranet.itemid_attrname_attrvalue;
CREATE TABLE  if not exists mpintranet.itemid_attrname_attrvalue(
itemid  BIGINT,
attrname  String,
attrvalue  String
)
CLUSTERED BY (itemid) INTO 783 BUCKETS
STORED AS ORC
;


insert into table mpintranet.itemid_attrname_attrvalue
select
a.itemid,
a.attrname,
nvl(b.attrvalue, '')
from
mpintranet.itemid_attrname a
left join
mpintranet.item_attr b
on a.itemid = b.itemid and a.attrname = b.attrname
;


---------- join 结果表
drop table if exists mpintranet.attrname_result;
CREATE TABLE  if not exists mpintranet.attrname_result(
itemid  BIGINT,
attrname  String,
attrvalue  String
)
CLUSTERED BY (itemid) INTO 113 BUCKETS
STORED AS ORC
;


insert into table mpintranet.attrname_result

select
t.itemid, group_concat(t.attrname, ",") as attrname, group_concat(t.attrvalue, ",") as attrvalue
from
(
SELECT
itemid, attrname, attrvalue
FROM
mpintranet.itemid_attrname_attrvalue
DISTRIBUTE BY itemid
SORT BY attrname desc
) t
group by t.itemid
;



------------end-------------

------------end-------------


drop table if exists mpintranet.attrname_export;
CREATE TABLE  if not exists mpintranet.attrname_export(
itemid  BIGINT,
attrvalue  String
)
CLUSTERED BY (itemid) INTO 113 BUCKETS
STORED AS ORC
;


insert into table mpintranet.attrname_export
select itemid, attrvalue from mpintranet.attrname_result
;







