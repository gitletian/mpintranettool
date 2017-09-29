--------------------------------------------------------------------------------------
----------                       计算买家网的 日库变
----------1、计算卖家网 的日库变 (从 ${hivevar:CURRENT_DAY}  至 2016-12-31)
----------2、初始化 ${hivevar:CURRENT_DAY} 的数据,
----------3、运行脚本 mjw_day_stock_change.sh

---------- 4、数据修复
----------       drop table if exists mpintranet.women_clothing_item_mjw_2;
----------       create table  mpintranet.women_clothing_item_mjw_2  like mpintranet.women_clothing_item_mjw;
----------
----------       insert into table mpintranet.women_clothing_item_mjw_2 PARTITION(daterange)
----------       select
----------         t1.itemid
----------       , t1.itemurl
----------       , t1.itemname
----------       , t1.itemsubtitle
----------       , t1.mainpicurl
----------       , t1.itemattrdesc
----------       , t1.listprice
----------       , t1.discountprice
----------       , t1.unitprice
----------       , t1.unit
----------       , t1.salesqty
----------       , t1.salesamt
----------       , t1.instock
----------       , t1.skulist
----------       , t1.favorites
----------       , t1.totalcomments
----------       , t1.brandname
----------       , t1.categoryid
----------       , t1.shopid
----------       , t1.adchannel
----------       , t1.platformid
----------       , t1.keyword
----------       , t1.monthlysalesqty
----------       , t1.totalsalesqty
----------       , t1.shopname
----------       , t1.categoryname
----------       , t1.listeddate
----------       , t2.sku_day_salesqty
----------       , t2.sku_day_salesamt
----------       , t2.sku_day_stock_change
----------       , t1.daterange
----------       from
----------       mpintranet.women_clothing_item_mjw t1
----------       left join
----------       mpintranet.women_clothing_item_mjw t2
----------       on date_sub(t1.daterange , 1) = t2.daterange and t1.itemid =t2.itemid
----------       ;

--------------------------------------------------------------------------------------

set mapred.fairscheduler.pool=poolHigh;
set ngmr.partition.automerge=true;
set ngmr.partition.mergesize.mb=200;


-- 3.1 总量去重表
/*


-- 永久表
drop table if exists mpintranet.women_clothing_item_unique_asc;
CREATE TABLE  if not exists mpintranet.women_clothing_item_unique_asc(
DateRange  DATE,
ItemID  BIGINT,
SKUList string
)
CLUSTERED BY (itemid) INTO 113 BUCKETS
STORED AS ORC;


-- 永久表
drop table if exists mpintranet.women_clothing_item_unique_desc;
CREATE TABLE  if not exists mpintranet.women_clothing_item_unique_desc(
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


-- 4 产生月库变

-- 4.1 最终数据表  永久表
drop table if exists mpintranet.women_clothing_item_mjw;
CREATE TABLE  if not exists mpintranet.women_clothing_item_mjw(
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
CLUSTERED BY (itemid) SORTED BY (categoryid) INTO 211 BUCKETS
STORED AS ORC;

 */



-------- 临时表    新增数据表     ---------begin
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
p.daterange,
p.ItemID,
p.ItemName,
p.ShopId,
p.ShopName,
p.PlatformId,
p.ItemAttrDesc,
p.CategoryId,
p.SKUList
from
(select daterange, ItemID, ItemName, shopID, ShopName, PlatformId, ItemAttrDesc, CategoryId, SKUList from elengjing.women_clothing_item_new_dict_20161231 where daterange = '${hivevar:CURRENT_DAY}') p
left join
mpintranet.women_clothing_item_unique_asc  d
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
mpintranet.women_clothing_item_unique_desc  d
left join
(select daterange, ItemID, shopID, ItemName, ItemAttrDesc, CategoryId, SKUList from elengjing.women_clothing_item_new_dict_20161231 where daterange = '${hivevar:CURRENT_DAY}') p
on p.ItemID = d.ItemID
where
p.ItemID is null
;





-------- 临时表    新增数据表     ---------end



------------ 计算日库变
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

add file /home/script/test/mpintranet/zzqs/mjw_day_stock_change.py;


insert into mpintranet.women_clothing_item_stock_change
select

TRANSFORM (p.ItemID, p.SKUList, y.SKUList, m.DateRange, p.DateRange)
USING "python mjw_day_stock_change.py"
AS (DateRange, ItemID, SKUList, sku_day_salesqty, sku_day_salesamt, sku_day_stock_change, is_has_months_change, error_info)

from
(select ItemID, SKUList, DateRange from elengjing.women_clothing_item_new_dict_20161231 where daterange = '${hivevar:CURRENT_DAY}') p
left join
( select DateRange, ItemID, SKUList from mpintranet.women_clothing_item_unique_desc where daterange > date_sub('${hivevar:CURRENT_DAY}', 31) ) y
on p.itemid = y.itemid
left join
( select DateRange, ItemID from mpintranet.women_clothing_item_unique_asc where daterange <= date_sub('${hivevar:CURRENT_DAY}', 31) ) m
on p.itemid = m.itemid
;



----插入汇总表


insert into mpintranet.women_clothing_item_mjw PARTITION(daterange)
select
 l.ItemID
,l.ItemUrl
,l.ItemName
,l.ItemSubTitle
,l.MainPicUrl
,l.ItemAttrDesc
,l.ListPrice
,l.discountprice
,l.UnitPrice
,l.Unit
,l.salesqty
,l.SalesAmt
,l.InStock
,l.SKUList
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
,l.Categoryname
,l.listeddate
,p.sku_day_salesqty
,p.sku_day_salesamt
,p.sku_day_stock_change
,l.DateRange

from
(select * from elengjing.women_clothing_item_new_dict_20161231 where daterange = '${hivevar:CURRENT_DAY}') l
left join
mpintranet.women_clothing_item_stock_change p
on l.DateRange = p.DateRange  and l.itemid = p.itemid
;




-- 9 维护总量去重表


-- 顺序去重表
insert into mpintranet.women_clothing_item_unique_asc
select
DateRange,
ItemID,
SKUList
from
mpintranet.women_clothing_item_new_item
;




-- 降序去重表

drop table if exists mpintranet.women_clothing_item_unique_desc_bak;
CREATE TABLE  if not exists mpintranet.women_clothing_item_unique_desc_bak(
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


insert into mpintranet.women_clothing_item_unique_desc_bak
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

insert into mpintranet.women_clothing_item_unique_desc_bak
select
DateRange,
ItemID,
shopID,
ItemName,
ItemAttrDesc,
CategoryId,
SKUList
from
(select daterange, ItemID, shopID, ItemName, ItemAttrDesc, CategoryId, SKUList from elengjing.women_clothing_item_new_dict_20161231 where daterange = '${hivevar:CURRENT_DAY}') p
;

drop table if exists mpintranet.women_clothing_item_unique_desc;
use mpintranet;
ALTER TABLE mpintranet.women_clothing_item_unique_desc_bak RENAME TO mpintranet.women_clothing_item_unique_desc;



