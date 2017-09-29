--------------------------------------------------------------------------------------
----------                       初始化第一天的 自主计算数据
----------1、需要首先初始化 卖家网 30天 数据的 日销量的 数据和 即:
---------- 				drop table if exists mpintranet.mjw_qut;
----------        CREATE TABLE  if not exists mpintranet.mjw_qut(
---------- 				itemid   BIGINT
---------- 				,amt   DECIMAL(20,2)
---------- 				,qut   BIGINT
----------        ,listeddate string
---------- 				)
---------- 				PARTITIONED BY (daterange date)
---------- 				CLUSTERED BY (itemid) INTO 17 BUCKETS
---------- 				STORED AS ORC;
----------
----------
---------- 				运行 sh:  sh mjw_qut_sum.sh "2017-01-03"
----------            时间为开始计算时间
----------2、运行beeline
---------- beeline -u jdbc:hive2://mphd02:10000 -n hive -p hive1  --hivevar CSV_ITEM_FILE=/home/data/women_clothing_item/item/hy_item_2017_01_04.csv  --hivevar BEGIN_DAY=2017-01-03  --hivevar DATE_RANGE=2017-01-03 -f  ./init_price.sql

----------3、 测试获取数据 wget --directory-prefix=/home/data/tmp/item/  ftp://139.224.59.29/*.zip --ftp-user=user1 --ftp-password=Marcpointuser1

----------4、 数据清洗 条件
----------  1. shopid 为空, 过滤,  占比万分之一以下
----------  2. categoryid 为空, 过滤, 占比 万分之一以下
----------  3、 price_list 为空, 过滤, 占比小于等于 百分之一(3号 1.05%,4号 0.4 % )
----------  4、 skulist 为空, 且 price_list不为空,sku_id 为空 ,过滤, 小于十万分之五(无法计算平均价,日库变等信息)
----------  5、 MonthlySalesQty 为空, 过滤, 占比 千分之1.5
----------  6、 PlatformId 不在 ("7001", "7002", "7011", "7012")
--------------------------------------------------------------------------------------

set mapred.fairscheduler.pool=poolHigh;
set ngmr.partition.automerge=true;
set ngmr.partition.mergesize.mb=200;

-- 1、数据入库
/**/
drop table if exists mpintranet.women_clothing_item_load;
CREATE TABLE  if not exists mpintranet.women_clothing_item_load(
DateRange  DATE,
ItemID  BIGINT,
ItemUrl  string,
ItemName  string,
ItemSubTitle  string,
MainPicUrl  string,
ItemAttrDesc  string,
ListPrice  DECIMAL(20,2),
DiscountPrice  DECIMAL(20,2),
UnitPrice  DECIMAL(20,2),
Unit  string,
SalesQty  string,
SalesAmt  string,
InStock  string,
SKUList  string,
Favorites  string,
TotalComments  string,
BrandName  string,
CategoryId  BIGINT,
ShopId  BIGINT,
AdChannel  string,
PlatformId  string,
keyword  string,
MonthlySalesQty  BIGINT,
TotalSalesQty  BIGINT,
ShopName  string,
Categoryname  string,
price_list  string,
listeddate  string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY'\t';


LOAD DATA LOCAL INPATH '${hivevar:CSV_ITEM_FILE}' OVERWRITE  INTO TABLE mpintranet.women_clothing_item_load;


-- 1.1 去重过滤
drop table if exists mpintranet.women_clothing_item_load_unique;
CREATE TABLE  if not exists mpintranet.women_clothing_item_load_unique(
DateRange  DATE,
ItemID  BIGINT,
ItemUrl  string,
ItemName  string,
ItemSubTitle  string,
MainPicUrl  string,
ItemAttrDesc  string,
ListPrice  DECIMAL(20,2),
DiscountPrice  DECIMAL(20,2),
UnitPrice  DECIMAL(20,2),
Unit  string,
SalesQty  string,
SalesAmt  string,
InStock  string,
SKUList  string,
Favorites  string,
TotalComments  string,
BrandName  string,
CategoryId  BIGINT,
ShopId  BIGINT,
AdChannel  string,
PlatformId  string,
keyword  string,
MonthlySalesQty  BIGINT,
TotalSalesQty  BIGINT,
ShopName  string,
Categoryname  string,
price_list  string,
listeddate  string
)
CLUSTERED BY (itemid) INTO 113 BUCKETS
STORED AS ORC;


insert into mpintranet.women_clothing_item_load_unique
select
 date_sub(a.DateRange, 1)
,a.ItemID
,a.ItemUrl
,a.ItemName
,a.ItemSubTitle
,a.MainPicUrl
,a.ItemAttrDesc
,a.ListPrice
,a.DiscountPrice
,a.UnitPrice
,a.Unit
,a.SalesQty
,a.SalesAmt
,a.InStock
,a.SKUList
,a.Favorites
,a.TotalComments
,a.BrandName
,a.CategoryId
,a.ShopId
,a.AdChannel
,a.PlatformId
,a.keyword
,a.MonthlySalesQty
,a.TotalSalesQty
,a.ShopName
,a.Categoryname
,a.price_list
,null
from mpintranet.women_clothing_item_load a
where
PlatformId in (7001, 7002, 7011, 7012)
-- and nvl(MonthlySalesQty, '') != ''
and nvl(price_list, '') != ''
and nvl(shopid, '') != ''
and nvl(CategoryId, '') != ''
and ( (nvl(SKUList, '') = '' and nvl(get_json_object(price_list, '$.prices[0].sku_id'), '') = '') or nvl(SKUList, '') != '')
and CategoryId in (1622,1623,1624,1629,1636,162103,162104,162105,162116,162201,162205,162401,162402,162403,162404,162701,162702,162703,50000671,50000697,50000852,50005065,50003509,50003510,50003511,50007068,50008897,50008898,50008899,50008900,50008901,50008903,50008904,50008905,50008906,50010850,50011277,50011404,50011411,50011412,50011413,50013194,50013196,50022566,50026651,121412004,121434004,123216004)
;




-- 2、对价格进行合并处理
drop table if exists mpintranet.women_clothing_item_combine_price;
CREATE TABLE  if not exists mpintranet.women_clothing_item_combine_price(
DateRange  DATE,
ItemID  BIGINT,
ItemName string,
ItemAttrDesc string,
ShopId  BIGINT,
ShopName  string,
PlatformId  string,
CategoryId BIGINT,
SKUList  string,
MonthlySalesQty  BIGINT,
DiscountPrice  DECIMAL(20,2),
error_info string
)
CLUSTERED BY (itemid) INTO 113 BUCKETS
STORED AS ORC;


add file /home/script/test/mpintranet/zzqs/combine_price_a7.py;

insert into mpintranet.women_clothing_item_combine_price
select
TRANSFORM (DateRange, ItemID, ItemName, ItemAttrDesc, ShopId, ShopName, PlatformId, CategoryId, SKUList, price_list, MonthlySalesQty, DiscountPrice, PlatformId)
USING "python combine_price_a7.py"
AS (DateRange, ItemID, ItemName, ItemAttrDesc, ShopId, ShopName, PlatformId, CategoryId, SKUList, MonthlySalesQty, DiscountPrice, error_info)
from
mpintranet.women_clothing_item_load_unique
;








-- 3 产生日库变

-- 3.1 总量去重表

-- 永久表
drop table if exists new_elengjing.women_clothing_item_unique_asc;
CREATE TABLE  if not exists new_elengjing.women_clothing_item_unique_asc(
DateRange  DATE,
ItemID  BIGINT,
SKUList string
)
CLUSTERED BY (itemid) INTO 113 BUCKETS
STORED AS ORC;


-- 永久表
drop table if exists new_elengjing.women_clothing_item_unique_desc;
CREATE TABLE  if not exists new_elengjing.women_clothing_item_unique_desc(
DateRange  DATE,
ItemID  BIGINT,
shopId BIGINT,
ItemName string,
ItemAttrDesc string,
CategoryId BIGINT,
SKUList string
)
CLUSTERED BY (itemid) INTO 113 BUCKETS
STORED AS ORC;

-- 临时表
drop table if exists mpintranet.women_clothing_item_new_item;
CREATE TABLE  if not exists mpintranet.women_clothing_item_new_item(
DateRange  DATE,
ItemID  BIGINT,
ItemName string,
ShopId  BIGINT,
ShopName  string,
PlatformId  string,
ItemAttrDesc string,
CategoryId BIGINT,
SKUList string
)
STORED AS ORC;



insert into mpintranet.women_clothing_item_new_item
select
p.DateRange,
p.ItemID,
p.ItemName,
p.ShopId,
p.ShopName,
p.PlatformId,
p.ItemAttrDesc,
p.CategoryId,
p.SKUList
from
mpintranet.women_clothing_item_combine_price p
left join
new_elengjing.women_clothing_item_unique_asc  d
on p.ItemID = d.ItemID
where
d.ItemID is null
;


-- 临时表
drop table if exists mpintranet.women_clothing_item_old_item;
CREATE TABLE  if not exists mpintranet.women_clothing_item_old_item(
DateRange  DATE,
ItemID  BIGINT,
shopid  BIGINT,
ItemName string,
ItemAttrDesc string,
CategoryId BIGINT,
SKUList string
)
STORED AS ORC;


insert into mpintranet.women_clothing_item_old_item
select
d.DateRange,
d.ItemID,
d.shopid,
d.ItemName,
d.ItemAttrDesc,
d.CategoryId,
d.SKUList
from
new_elengjing.women_clothing_item_unique_desc  d
left join
mpintranet.women_clothing_item_combine_price p
on p.ItemID = d.ItemID
where
p.ItemID is null
;

/**/



-- 3.2 计算日库变

drop table if exists mpintranet.women_clothing_item_stock_change;
CREATE TABLE  if not exists mpintranet.women_clothing_item_stock_change(
DateRange  DATE,
ItemID  BIGINT,
SKUList  string,
sku_day_salesqty BIGINT,
sku_day_salesamt DECIMAL(20,2),
sku_day_stock_change string,
is_has_months_change BIGINT,
error_info string
)
CLUSTERED BY (itemid) INTO 113 BUCKETS
STORED AS ORC;

add file /home/script/test/mpintranet/zzqs/day_stock_change7.py;


insert into mpintranet.women_clothing_item_stock_change
select

TRANSFORM (p.ItemID, p.SKUList, y.SKUList, m.DateRange, p.DateRange)
USING "python day_stock_change7.py"
AS (DateRange, ItemID, SKUList, sku_day_salesqty, sku_day_salesamt, sku_day_stock_change, is_has_months_change, error_info)

from
mpintranet.women_clothing_item_combine_price p
left join
( select DateRange, ItemID, SKUList from new_elengjing.women_clothing_item_unique_desc where daterange > date_sub('${hivevar:DATE_RANGE}', 31) ) y
on p.itemid = y.itemid
left join
( select DateRange, ItemID from new_elengjing.women_clothing_item_unique_asc where daterange <= date_sub('${hivevar:DATE_RANGE}', 31) ) m
on p.itemid = m.itemid
;


-- 4 产生月库变

-- 4.1 最终数据表  永久表
drop table if exists new_elengjing.women_clothing_item;
CREATE TABLE  if not exists new_elengjing.women_clothing_item(
ItemID  BIGINT,
ItemUrl  string,
ItemName  string,
ItemSubTitle  string,
MainPicUrl  string,
ItemAttrDesc  string,
ListPrice  DECIMAL(20,2),
DiscountPrice  DECIMAL(20,2),
UnitPrice  DECIMAL(20,2),
Unit  string,
SalesQty  string,
SalesAmt  string,
InStock  string,
SKUList  string,
Favorites  string,
TotalComments  string,
BrandName  string,
CategoryId  BIGINT,
ShopId  BIGINT,
AdChannel  string,
PlatformId  string,
keyword  string,
MonthlySalesQty  BIGINT,
TotalSalesQty  BIGINT,
ShopName  string,
Categoryname  string,
listeddate  string,
sku_day_salesqty BIGINT,
sku_day_salesamt DECIMAL(20,2),
sku_day_stock_change string
)
PARTITIONED BY RANGE (DateRange DATE)(
PARTITION 2015_01 VALUES LESS THAN ('2015-02-01'),
PARTITION 2015_02 VALUES LESS THAN ('2015-03-01'),
PARTITION 2015_03 VALUES LESS THAN ('2015-04-01'),
PARTITION 2015_04 VALUES LESS THAN ('2015-05-01'),
PARTITION 2015_05 VALUES LESS THAN ('2015-06-01'),
PARTITION 2015_06 VALUES LESS THAN ('2015-07-01'),
PARTITION 2015_07 VALUES LESS THAN ('2015-08-01'),
PARTITION 2015_08 VALUES LESS THAN ('2015-09-01'),
PARTITION 2015_09 VALUES LESS THAN ('2015-10-01'),
PARTITION 2015_10 VALUES LESS THAN ('2015-11-01'),
PARTITION 2015_11 VALUES LESS THAN ('2015-12-01'),
PARTITION 2015_12 VALUES LESS THAN ('2016-01-01'),
PARTITION 2016_01 VALUES LESS THAN ('2016-02-01'),
PARTITION 2016_02 VALUES LESS THAN ('2016-03-01'),
PARTITION 2016_03 VALUES LESS THAN ('2016-04-01'),
PARTITION 2016_04 VALUES LESS THAN ('2016-05-01'),
PARTITION 2016_05 VALUES LESS THAN ('2016-06-01'),
PARTITION 2016_06 VALUES LESS THAN ('2016-07-01'),
PARTITION 2016_07 VALUES LESS THAN ('2016-08-01'),
PARTITION 2016_08 VALUES LESS THAN ('2016-09-01'),
PARTITION 2016_09 VALUES LESS THAN ('2016-10-01'),
PARTITION 2016_10 VALUES LESS THAN ('2016-11-01'),
PARTITION 2016_11 VALUES LESS THAN ('2016-12-01'),
PARTITION 2016_12 VALUES LESS THAN ('2017-01-01'),
PARTITION 2017_01 VALUES LESS THAN ('2017-02-01'),
PARTITION 2017_02 VALUES LESS THAN ('2017-03-01'),
PARTITION 2017_03 VALUES LESS THAN ('2017-04-01'),
PARTITION 2017_04 VALUES LESS THAN ('2017-05-01'),
PARTITION 2017_05 VALUES LESS THAN ('2017-06-01'),
PARTITION 2017_06 VALUES LESS THAN ('2017-07-01'),
PARTITION 2017_07 VALUES LESS THAN ('2017-08-01'),
PARTITION 2017_08 VALUES LESS THAN ('2017-09-01'),
PARTITION 2017_09 VALUES LESS THAN ('2017-10-01'),
PARTITION 2017_10 VALUES LESS THAN ('2017-11-01'),
PARTITION 2017_11 VALUES LESS THAN ('2017-12-01'),
PARTITION 2017_12 VALUES LESS THAN ('2018-01-01')
)
CLUSTERED BY (shopid) SORTED BY (categoryid) INTO 211 BUCKETS
STORED AS ORC;





-- 4.2 产生月库变
add jar /home/script/test/mpintranet/zzqs/skumonthchange1.jar;
drop temporary function if exists test_udaf4;
create temporary function test_udaf4 as 'com.marcpoint.elengjing.SkuMonthChange1';


drop table if exists mpintranet.women_clothing_item_months_stock_change;
CREATE TABLE  if not exists mpintranet.women_clothing_item_months_stock_change(
DateRange  DATE,
ItemID  BIGINT,
sku_months_stock_change string,
sku_months_salesqty BIGINT,
sku_months_salesamt DECIMAL(20,2)
)
CLUSTERED BY (itemid) INTO 113 BUCKETS
STORED AS ORC;


insert into mpintranet.women_clothing_item_months_stock_change
select
'${hivevar:DATE_RANGE}',
t.itemid,
test_udaf4(t.sku_day_stock_change),
sum(nvl(t.sku_day_salesqty, 0)),
sum(nvl(t.sku_day_salesamt, 0))
from
(
select itemid, sku_day_stock_change, sku_day_salesqty, sku_day_salesamt  from new_elengjing.women_clothing_item where  daterange BETWEEN date_sub('${hivevar:DATE_RANGE}', 29) and '${hivevar:DATE_RANGE}'
union all
select itemid, sku_day_stock_change, sku_day_salesqty, sku_day_salesamt from mpintranet.women_clothing_item_stock_change
) t
group by itemid;



-- 5 计算日销量
-- 5.1
drop table if exists mpintranet.women_clothing_item_day_salesqty;
CREATE TABLE  if not exists mpintranet.women_clothing_item_day_salesqty(
 itemid   BIGINT
,SalesQty   BIGINT
,daterange STRING
,DiscountPrice DECIMAL(20,2)
,listeddate STRING
)
CLUSTERED BY (itemid) INTO 113 BUCKETS
STORED AS ORC;


-------------------DATE_RANGE 日数据 -------------------
-- 5.3

with day_sum as
(
select
itemid,
sum(salesqty) as qut,
min(if(listeddate = '', null, listeddate)) as listeddate
from
new_elengjing.women_clothing_item
where daterange BETWEEN date_sub('${hivevar:DATE_RANGE}', 29) and date_sub('${hivevar:DATE_RANGE}', 1)
group by itemid
)

insert into mpintranet.women_clothing_item_day_salesqty
select
n.itemid,
n.MonthlySalesQty - nvl(s.qut,0) - nvl(t.qut,0),
n.daterange,
n.DiscountPrice,
nvl(t.listeddate, nvl(s.listeddate, '${hivevar:DATE_RANGE} 00:00:00'))


from
mpintranet.women_clothing_item_combine_price n
left join
(select * from mpintranet.mjw_qut where  daterange = cast('${hivevar:DATE_RANGE}' as date) ) s
on n.itemid = s.itemid
left join
day_sum t
on n.itemid = t.itemid
;



-- 30 天之后

/*
with day_sum as
(
select
itemid,
sum(salesqty) as qut,
min(listeddate) as listeddate
from
new_elengjing.women_clothing_item
where daterange BETWEEN cast('${hivevar:BEGIN_DAY}' as date) and date_sub('${hivevar:DATE_RANGE}', 1)
group by itemid
)

insert into mpintranet.women_clothing_item_day_salesqty
select
n.itemid,
n.MonthlySalesQty - nvl(t.qut,0),
n.daterange,
n.DiscountPrice,
min(nvl(t.listeddate, '${hivevar:DATE_RANGE}'), '${hivevar:DATE_RANGE}')
from
mpintranet.women_clothing_item_combine_price n
left join
day_sum t
on n.itemid = t.itemid
;
*/




-- 6 产生spu 价格
drop table if exists mpintranet.women_clothing_item_spu_price;
CREATE TABLE  if not exists mpintranet.women_clothing_item_spu_price(
DateRange  DATE,
ItemID  BIGINT,
sku_day_salesqty BIGINT,
sku_day_salesamt DECIMAL(20,2),
sku_day_stock_change string,
spu_price DECIMAL(20,2),
SKUList string,
SalesQty INT,
listeddate string,
error_info string
)
CLUSTERED BY (itemid) INTO 113 BUCKETS
STORED AS ORC;

add file /home/script/test/mpintranet/zzqs/spu_price17.py;

insert into mpintranet.women_clothing_item_spu_price

select
TRANSFORM (d.ItemID, d.SKUList, d.sku_day_salesqty, d.sku_day_salesamt, m.sku_months_salesqty, m.sku_months_salesamt, d.sku_day_stock_change, m.sku_months_stock_change, s.SalesQty, s.listeddate, d.is_has_months_change, d.DateRange)
USING "python spu_price17.py"
AS (DateRange, ItemID, sku_day_salesqty, sku_day_salesamt, sku_day_stock_change, spu_price, SKUList, SalesQty, listeddate, error_info)

from
mpintranet.women_clothing_item_stock_change d
left join
mpintranet.women_clothing_item_months_stock_change m
on d.itemid = m.itemid
left join
mpintranet.women_clothing_item_day_salesqty s
on d.itemid = s.itemid
-- where nvl(s.SalesQty, d.sku_day_salesqty) > 0
;





-- 7 最终数据表  永久表

insert into new_elengjing.women_clothing_item PARTITION(daterange)
select
 l.ItemID
,l.ItemUrl
,l.ItemName
,l.ItemSubTitle
,l.MainPicUrl
,l.ItemAttrDesc
,l.ListPrice
,p.spu_price
,l.UnitPrice
,l.Unit
,nvl(p.salesqty, p.sku_day_salesqty)
,p.spu_price *  nvl(p.salesqty, p.sku_day_salesqty)
,l.InStock
,p.SKUList
,l.Favorites
,l.TotalComments
,l.BrandName
,l.CategoryId
,l.ShopId
,l.AdChannel
,l.PlatformId
,l.keyword
,l.MonthlySalesQty
,l.TotalSalesQty
,l.ShopName
,c.Categoryname
,p.listeddate
,p.sku_day_salesqty
,p.sku_day_salesamt
,p.sku_day_stock_change
,l.DateRange

from
mpintranet.women_clothing_item_load_unique l
left join
mpintranet.women_clothing_item_spu_price p
on l.itemid = p.itemid
left join
elengjing.category c
on l.CategoryId = c.categoryid
where p.itemid is not null
;







-- 8 打标签
/*
drop table if exists mpintranet.women_clothing_item_attr;
CREATE TABLE if not exists mpintranet.women_clothing_item_attr(
itemid BIGINT,
attrname STRING,
attrvalue STRING,
errormessage STRING
)
CLUSTERED BY (ItemID) SORTED BY (ItemID) INTO 113 BUCKETS
STORED AS ORC;

add jar /home/script/normal_servers/serverudf/elengjing/tagged.jar;

insert into mpintranet.women_clothing_item_attr
select
Tagged(tw.ItemID,tw.categoryid,tw.ItemName,tw.ItemAttrDesc)  as (itemid,attrname,attrvalue,errormessage)
from
mpintranet.women_clothing_item_new_item tw
;





CREATE TABLE if not exists mpintranet.women_clothing_item_attr_end (
ItemID BIGINT,
AttrValue STRING
)
PARTITIONED BY(AttrName STRING)
CLUSTERED BY (AttrValue) SORTED BY (ItemID) INTO 31 BUCKETS
STORED AS ORC;


INSERT INTO table mpintranet.women_clothing_item_attr_end PARTITION(AttrName)
SELECT ItemID, AttrValue, AttrName
from mpintranet.women_clothing_item_attr
where ErrorMessage is null or ErrorMessage="";



CREATE TABLE if not exists mpintranet.women_clothing_item_attr_error (
ItemID BIGINT,
AttrName STRING,
AttrValue STRING,
ErrorMessage STRING
)
CLUSTERED BY (ItemID) SORTED BY (ItemID) INTO 113 BUCKETS
STORED AS ORC;


INSERT INTO table mpintranet.women_clothing_item_attr_error
SELECT ta.ItemID,ta.AttrName,ta.AttrValue,ta.ErrorMessage
from mpintranet.women_clothing_item_attr ta
where ta.ErrorMessage is not null and ta.ErrorMessage!="";
*/

-- 9 维护总量去重表


-- 顺序去重表
insert into new_elengjing.women_clothing_item_unique_asc
select
DateRange,
ItemID,
SKUList
from
mpintranet.women_clothing_item_new_item
;




-- 降序去重表

drop table if exists new_elengjing.women_clothing_item_unique_desc_bak;
CREATE TABLE  if not exists new_elengjing.women_clothing_item_unique_desc_bak(
DateRange  DATE,
ItemID  BIGINT,
shopId BIGINT,
ItemName string,
ItemAttrDesc string,
CategoryId BIGINT,
SKUList string
)
CLUSTERED BY (itemid) INTO 113 BUCKETS
STORED AS ORC;


insert into new_elengjing.women_clothing_item_unique_desc_bak
select
DateRange,
ItemID,
shopId,
ItemName,
ItemAttrDesc,
CategoryId,
SKUList
from
mpintranet.women_clothing_item_old_item
;

insert into new_elengjing.women_clothing_item_unique_desc_bak
select
DateRange,
ItemID,
shopID,
ItemName,
ItemAttrDesc,
CategoryId,
SKUList
from
mpintranet.women_clothing_item_combine_price
;

drop table if exists new_elengjing.women_clothing_item_unique_desc;
use new_elengjing;
ALTER TABLE new_elengjing.women_clothing_item_unique_desc_bak RENAME TO new_elengjing.women_clothing_item_unique_desc;

