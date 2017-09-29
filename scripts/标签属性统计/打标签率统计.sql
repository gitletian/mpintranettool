----1、 拆分 desc
drop table if exists mpintranet.desc_attr_count_tmp;
CREATE TABLE if not exists mpintranet.desc_attr_count_tmp(
itemid BIGINT,
shopid BIGINT,
categoryid BIGINT,
attrname STRING,
attrvalue STRING,
errormessage STRING
)
CLUSTERED BY (ItemID) SORTED BY (ItemID) INTO 871 BUCKETS
STORED AS ORC
;


add jar /home/script/normal_servers/serverudf/elengjing/Tag_13.jar;

drop temporary function if exists AttrCount_o_13;
create temporary function AttrCount_o_13 as 'com.marcpoint.elengjing_new_2.AttrCount_o_13';

insert into mpintranet.desc_attr_count_tmp
select
AttrCount_o_13(tw.ItemID,tw.categoryid,tw.shopid,tw.itemname,regexp_replace(tw.ItemAttrDesc, '\t', ' '))  as (itemid,shopid,categoryid,attrname,attrvalue,errormessage)
from
elengjing.women_clothing_item_unique tw
;



add jar /home/script/normal_servers/serverudf/elengjing/Tag_13.jar;

drop temporary function if exists AttrCount_n_13;
create temporary function AttrCount_n_13 as 'com.marcpoint.elengjing_new_2.AttrCount_n_13';

insert into mpintranet.desc_attr_count_tmp
select
AttrCount_n_13(tw.ItemID,tw.categoryid,tw.shopid,tw.itemname,regexp_replace(tw.ItemAttrDesc, '\t', ' '))  as (itemid,shopid,categoryid,attrname,attrvalue,errormessage)
from
new_elengjing.women_clothing_item_unique_desc tw
;




drop table if exists mpintranet.desc_attr_count;
CREATE TABLE if not exists mpintranet.desc_attr_count(
itemid BIGINT,
shopid BIGINT,
categoryid BIGINT,
attrname STRING,
attrvalue STRING,
errormessage STRING
)
CLUSTERED BY (ItemID) SORTED BY (ItemID) INTO 873 BUCKETS
STORED AS ORC
;

insert into table mpintranet.desc_attr_count
SELECT
*
FROM
mpintranet.desc_attr_count_tmp
where length(attrname) < 20
;




--------数据打标签--------------

--------旧方式打标签-------------- mpintranet.women_clothing_item_attr
--重新打标签


drop table if exists mpintranet.women_clothing_item_attr_old;
CREATE TABLE if not exists mpintranet.women_clothing_item_attr_old(
itemid BIGINT,
shopid BIGINT,
categoryid BIGINT,
attrname STRING,
attrvalue STRING,
errormessage STRING
)
CLUSTERED BY (ItemID) SORTED BY (ItemID) INTO 113 BUCKETS
STORED AS ORC;

add jar /home/script/normal_servers/serverudf/elengjing/Tag_13.jar;

drop temporary function if exists Tagged_Old_13;
create temporary function Tagged_Old_13 as 'com.marcpoint.elengjing.Tagged_Old_13';

insert into mpintranet.women_clothing_item_attr_old
select
Tagged_Old_13(tw.ItemID,tw.categoryid,tw.shopid,tw.itemname,regexp_replace(tw.ItemAttrDesc, '\t', ' '))  as (itemid,shopid,categoryid,attrname,attrvalue,errormessage)
from
elengjing.women_clothing_item_unique tw
;



-- 8、新表进行打标签

drop table if exists mpintranet.women_clothing_item_attr_new;
CREATE TABLE if not exists mpintranet.women_clothing_item_attr_new(
itemid BIGINT,
shopid BIGINT,
categoryid BIGINT,
attrname STRING,
attrvalue STRING,
errormessage STRING
)
CLUSTERED BY (ItemID) SORTED BY (ItemID) INTO 113 BUCKETS
STORED AS ORC;

add jar /home/script/normal_servers/serverudf/elengjing/Tag_13.jar;

drop temporary function if exists Tagged_New_13;
create temporary function Tagged_New_13 as 'com.marcpoint.elengjing_new.Tagged_New_13';

insert into mpintranet.women_clothing_item_attr_new
select
Tagged_New_13(tw.ItemID,tw.categoryid,tw.shopid,tw.itemname,regexp_replace(tw.ItemAttrDesc, '\t', ' '))  as (itemid,shopid,categoryid,attrname,attrvalue,errormessage)
from
new_elengjing.women_clothing_item_unique_desc tw
;





drop table if exists mpintranet.women_clothing_item_attr;
CREATE TABLE if not exists mpintranet.women_clothing_item_attr (
ItemID BIGINT,
shopid BIGINT,
categoryid BIGINT,
attrname STRING,
AttrValue STRING
)
PARTITIONED BY(AttrName STRING)
CLUSTERED BY (AttrValue) SORTED BY (ItemID) INTO 31 BUCKETS
STORED AS ORC;

INSERT INTO table mpintranet.women_clothing_item_attr PARTITION(AttrName)
SELECT ItemID,shopid,categoryid, AttrValue, AttrName
from mpintranet.women_clothing_item_attr_new
where ErrorMessage is null or ErrorMessage="";

INSERT INTO table mpintranet.women_clothing_item_attr PARTITION(AttrName)
SELECT ItemID,shopid,categoryid, AttrValue, AttrName
from mpintranet.women_clothing_item_attr_old
where ErrorMessage is null or ErrorMessage="";




--------新方式打标签--------------  elengjing.women_clothing_item_attr


drop table if exists elengjing.women_clothing_item_attr_old;
CREATE TABLE if not exists elengjing.women_clothing_item_attr_old(
itemid BIGINT,
shopid BIGINT,
categoryid BIGINT,
attrname STRING,
attrvalue STRING,
errormessage STRING
)
CLUSTERED BY (ItemID) SORTED BY (ItemID) INTO 113 BUCKETS
STORED AS ORC;

add jar /home/script/normal_servers/serverudf/elengjing/Tag_13.jar;

drop temporary function if exists Tagged_o_13;
create temporary function Tagged_o_13 as 'com.marcpoint.elengjing_new_2.Tagged_o_13';

insert into elengjing.women_clothing_item_attr_old
select
Tagged_o_13(tw.ItemID,tw.categoryid,tw.shopid,tw.itemname,regexp_replace(tw.ItemAttrDesc, '\t', ' '))  as (itemid,shopid,categoryid,attrname,attrvalue,errormessage)
from
elengjing.women_clothing_item_unique tw
;



-- 8、新表进行打标签

drop table if exists elengjing.women_clothing_item_attr_new;
CREATE TABLE if not exists elengjing.women_clothing_item_attr_new(
itemid BIGINT,
shopid BIGINT,
categoryid BIGINT,
attrname STRING,
attrvalue STRING,
errormessage STRING
)
CLUSTERED BY (ItemID) SORTED BY (ItemID) INTO 113 BUCKETS
STORED AS ORC;

add jar /home/script/normal_servers/serverudf/elengjing/Tag_13.jar;

drop temporary function if exists Tagged_n_13;
create temporary function Tagged_n_13 as 'com.marcpoint.elengjing_new_2.Tagged_n_13';

insert into elengjing.women_clothing_item_attr_new
select
Tagged_n_13(tw.ItemID,tw.categoryid,tw.shopid,tw.itemname,regexp_replace(tw.ItemAttrDesc, '\t', ' '))  as (itemid,shopid,categoryid,attrname,attrvalue,errormessage)
from
new_elengjing.women_clothing_item_unique_desc tw
;



drop table if exists elengjing.women_clothing_item_attr;
CREATE TABLE if not exists elengjing.women_clothing_item_attr (
ItemID BIGINT,
shopid BIGINT,
categoryid BIGINT,
AttrValue STRING
)
PARTITIONED BY(AttrName STRING)
CLUSTERED BY (AttrValue) SORTED BY (ItemID) INTO 31 BUCKETS
STORED AS ORC;

INSERT INTO table elengjing.women_clothing_item_attr PARTITION(AttrName)
SELECT ItemID,shopid,categoryid, AttrValue, AttrName
from elengjing.women_clothing_item_attr_new
where ErrorMessage is null or ErrorMessage="";

INSERT INTO table elengjing.women_clothing_item_attr PARTITION(AttrName)
SELECT ItemID,shopid,categoryid, AttrValue, AttrName
from elengjing.women_clothing_item_attr_old
where ErrorMessage is null or ErrorMessage="";





--------------------------------------------统计------------------------------------------------


----2、 按品类统计

drop table if exists mpintranet.category_comparison;
CREATE TABLE if not exists mpintranet.category_comparison(
categoryid BIGINT,
attrname STRING,
d_attrvalue STRING,
d_ct STRING,
t_attrvalue STRING,
t_ct STRING
)
CLUSTERED BY (categoryid) INTO 113 BUCKETS
STORED AS ORC;



with taged_attr as (
select
categoryid, attrname, group_concat(distinct attrvalue, ",") as attrvalue, count(distinct attrvalue) as ct
from
mpintranet.women_clothing_item_attr group by categoryid, attrname
),
desc_attr as (
select
categoryid, attrname, group_concat(distinct attrvalue, ",") as attrvalue, count(distinct attrvalue) as ct
from
mpintranet.desc_attr_count group by categoryid, attrname
)

insert into table mpintranet.category_comparison

select
d.categoryid,
d.attrname,
d.attrvalue,
d.ct,
t.attrvalue,
t.ct

from
desc_attr d
left join
taged_attr t
on d.categoryid = t.categoryid and d.attrname = t.attrname
;


----3、 按属性名统计

drop table if exists mpintranet.attrname_comparison;
CREATE TABLE if not exists mpintranet.attrname_comparison(
attrname STRING,
d_attrvalue STRING,
d_ct STRING,
t_attrvalue STRING,
t_ct STRING
)
CLUSTERED BY (attrname) INTO 113 BUCKETS
STORED AS ORC;



with taged_attr as (
select
attrname, group_concat(distinct attrvalue, ",") as attrvalue, count(distinct attrvalue) as ct
from
mpintranet.women_clothing_item_attr group by attrname
),
desc_attr as (
select
attrname, group_concat(distinct attrvalue, ",") as attrvalue, count(distinct attrvalue) as ct
from
mpintranet.desc_attr_count group by attrname
)

insert into table mpintranet.attrname_comparison

select
d.attrname,
d.attrvalue,
d.ct,
t.attrvalue,
t.ct

from
desc_attr d
left join
taged_attr t
on d.attrname = t.attrname
;



----4、 按店铺统计

drop table if exists mpintranet.desc_att_shop_comparison;
CREATE TABLE if not exists mpintranet.desc_att_shop_comparison(
shopid BIGINT,
attrname STRING,
d_attrvalue STRING,
d_ct STRING,
t_attrvalue STRING,
t_ct STRING
)
CLUSTERED BY (attrname) SORTED BY (attrname) INTO 113 BUCKETS
STORED AS ORC;



with taged_attr as (
select
shopid, attrname, group_concat(distinct attrvalue, ",") as attrvalue, count(distinct attrvalue) as ct
from
mpintranet.women_clothing_item_attr where shopid in (73401272, 66098091, 69302618) group by shopid, attrname
),
desc_attr as (
select
shopid, attrname, group_concat(distinct attrvalue, ",") as attrvalue, count(distinct attrvalue) as ct
from
mpintranet.desc_attr_count where shopid in (73401272, 66098091, 69302618) group by shopid, attrname
)


insert into table mpintranet.desc_att_shop_comparison
select
d.shopid,
d.attrname,
d.attrvalue as d_attrvalue,
d.ct as d_ct,
t.attrvalue as t_attrvalue,
t.ct as t_ct

from
desc_attr d
left join
taged_attr t
on d.shopid = t.shopid and d.attrname = t.attrname
;


------3、 报表展示


SELECT
attrname,
d_ct,
t_ct,
d_attrvalue,
t_attrvalue
FROM
 mpintranet.attrname_comparison

 where length(attrname) < 20
;






with base as (
SELECT
categoryid,
attrname,
d_ct,
t_ct,
d_attrvalue,
t_attrvalue
FROM
 mpintranet.category_comparison

)
SELECT
c.categoryname,
b.*
from
base b
left JOIN
elengjing.category c
on b.categoryid = c.categoryid
;



select
shopid,
attrname
d_ct,
t_ct
FROM
mpintranet.desc_att_shop_comparison
where shopid in (73401272, 66098091, 69302618)
;



--- 导出
---  _1 为 达标率统计 用的, 是用最原始的 打标签的方法打标签。 下面是用最新的打标签的方法打标签
with base as (
SELECT
categoryid,
attrname,
d_ct,
t_ct,
untag
FROM
 mpintranet.untag

)

SELECT
c.categoryname,
b.categoryid,
b.d_ct,
b.t_ct,
b.attrname,
substr(b.untag, 0, 5000) as untag
from
base b
left JOIN
elengjing.category c
on b.categoryid = c.categoryid
where length(b.attrname) < 20
;

----------------



with base as (
SELECT
categoryid,
attrname,
d_ct,
t_ct,
d_attrvalue,
t_attrvalue
FROM
 mpintranet.category_comparison

)


insert overwrite local directory "/home/data/tmp/category_comparison"
row format delimited
fields terminated by "\t"
SELECT
c.categoryname,
b.categoryid,
regexp_replace(b.attrname, '(\\r)|(\\n)', ''),
b.d_ct,
b.t_ct,
regexp_replace(b.d_attrvalue, '\t', ''),
regexp_replace(b.t_attrvalue, '\t', '')

from
base b
left JOIN
elengjing.category c
on b.categoryid = c.categoryid
where length(b.attrname) < 20
;


--- 打标率 统计
drop table if exists mpintranet.category_comparison_ratio;
CREATE TABLE if not exists mpintranet.category_comparison_ratio(
categoryid BIGINT,
attrname STRING,
desc_count STRING,
untag_count STRING,
untag_value STRING,
error_info STRING
)
STORED AS ORC
;


add file /home/test/untag_3.py;

insert into mpintranet.category_comparison_ratio
SELECT
  TRANSFORM (categoryid, d_ct, t_ct, regexp_replace(d_attrvalue, '\t', ' '), regexp_replace(t_attrvalue, '\t', ' '), attrname)
  USING 'python untag_3.py'
  AS (categoryid, attrname, desc_count, untag_count, untag_value, error_info)
FROM mpintranet.category_comparison
;




--------------需要新添加的 标签-------------------------
insert overwrite local directory "/home/data/tmp/category_comparison_ratio"
row format delimited
fields terminated by "\t"
select * from mpintranet.category_comparison_ratio
where
1 = 1
and length(attrname) < 20
and TRIM(nvl(untag_value, '')) != ''
and attrname not in ("克重", "品牌", "货号", "尺寸", "尺码", "颜色", "主要颜色", "颜色分类", "\"颜色分类",
                     "通勤", "甜美", "街头",
                     "适用人群", "特别提示", "商品描述", "使用方法", "主要成分与功效", "XL码", "S码", "M码", "XL码", "XL码",
                     "备注", "产品名称", "是否新品", "系列名", "裤尺寸", "适用性别",
                     "模特信息", "模特穿着尺寸", "国内参考价", "单位",
                    "\"货号", "L码", "XXL码", "\"品牌", "品牌属地", "货源地", "")
                    order by attrname DESC
;





--------------------------

