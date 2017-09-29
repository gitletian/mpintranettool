--------------------------------------------------------------------------------------
----------                        women_clothing_item 入库脚本
----------1、item的定时入库
----------2、增量item的打标签
----------
--------------------------------------------------------------------------------------
set mapred.fairscheduler.pool=poolHigh;
set ngmr.partition.automerge=true;
set ngmr.partition.mergesize.mb=200;
-------------------create e_women_clothing_item ------------------------

--drop table if exists elengjing.e_women_clothing_item;

CREATE EXTERNAL TABLE  if not exists e_elengjing.women_clothing_item(
DateRange DATE,
ItemID BIGINT,
ItemName STRING,
ItemUrl STRING,
ListPrice DECIMAL(20,2),
DiscountPrice DECIMAL(20,2),
SalesQty BIGINT,
SalesAmt DECIMAL(20,2),
ShopID BIGINT,
AdChannel STRING,
BrandName STRING,
MainPicUrl STRING,
InStock STRING,
SKUList STRING,
CategoryID BIGINT,
ItemAttrDesc STRING,
Favorites STRING,
TotalComments STRING,
ListedDate STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY'\001';


----------------------------load data-----------------------------------------------

LOAD DATA LOCAL INPATH '${hivevar:CSV_ITEM_FILE}' OVERWRITE INTO TABLE e_elengjing.women_clothing_item;
---LOAD DATA LOCAL INPATH '/home/data/women_clothing_item/hy_item/hy_item_2016_09_30.csv' OVERWRITE INTO TABLE e_elengjing.women_clothing_item;

----------------------------create orc table --------------------------------------------------------
drop table if exists t_elengjing.women_clothing_item;
---truncate table t_elengjing.women_clothing_item;
CREATE TABLE if not exists t_elengjing.women_clothing_item (
DateRange DATE,
ItemID BIGINT,
ItemName STRING,
ItemUrl STRING,
ListPrice DECIMAL(20,2),
DiscountPrice DECIMAL(20,2),
SalesQty BIGINT,
SalesAmt DECIMAL(20,2),
ShopID BIGINT,
AdChannel STRING,
BrandName STRING,
MainPicUrl STRING,
InStock STRING,
SKUList STRING,
CategoryID BIGINT,
ItemAttrDesc STRING,
Favorites STRING,
TotalComments STRING,
ListedDate STRING
)
STORED AS ORC;



INSERT INTO table t_elengjing.women_clothing_item 
SELECT 
t.DateRange,
t.ItemID,
t.ItemName,
t.ItemUrl,
t.ListPrice,
t.DiscountPrice,
t.SalesQty,
t.SalesAmt,
t.ShopID,
t.AdChannel,
t.BrandName,
t.MainPicUrl,
t.InStock,
t.SKUList,
t.CategoryID,
t.ItemAttrDesc,
t.Favorites,
t.TotalComments,
t.ListedDate
FROM 
(
select 
* 
from
(
select tw1.* ,ROW_NUMBER() OVER ( Partition By itemid,DateRange) AS rn
from e_elengjing.women_clothing_item tw1 
) tw where tw.rn=1
) t;



--------------------------------------------------add item desc--------------------------------------------------------------------------
/**/

drop table if exists t_elengjing.women_clothing_item_attr;
CREATE TABLE if not exists t_elengjing.women_clothing_item_attr(
itemid BIGINT,
attrname STRING,
attrvalue STRING,
errormessage STRING
)
CLUSTERED BY (ItemID) SORTED BY (ItemID) INTO 113 BUCKETS
STORED AS ORC;

add jar /home/script/normal_servers/serverudf/elengjing/tagged.jar;

insert into t_elengjing.women_clothing_item_attr
select 
Tagged(tw.ItemID,tw.categoryid,tw.ItemName,tw.ItemAttrDesc)  as (itemid,attrname,attrvalue,errormessage)
from 
(select * from 
(
select tw1.* ,ROW_NUMBER() OVER ( Partition By itemid order by DateRange desc) AS rn
from t_elengjing.women_clothing_item tw1 
)  t where t.rn=1) tw
left join 
elengjing.women_clothing_item_id  a2
on tw.ItemID = a2.ItemID
where a2.ItemID is null ;


-------------------------women_clothing_item_attr-------------------------

CREATE TABLE if not exists elengjing.women_clothing_item_attr (
ItemID BIGINT,
AttrValue STRING
)
PARTITIONED BY(AttrName STRING)
CLUSTERED BY (AttrValue) SORTED BY (ItemID) INTO 31 BUCKETS
STORED AS ORC;


INSERT INTO table elengjing.women_clothing_item_attr PARTITION(AttrName)
SELECT ItemID, AttrValue, AttrName
from t_elengjing.women_clothing_item_attr
where ErrorMessage is null or ErrorMessage="";


-------------------------women_clothing_item_id-------------------------

CREATE TABLE if not exists elengjing.women_clothing_item_id (
ItemID BIGINT
)
CLUSTERED BY (ItemID) SORTED BY (ItemID) INTO 71 BUCKETS
STORED AS ORC;


insert into 
elengjing.women_clothing_item_id
select 
itemid 
from t_elengjing.women_clothing_item_attr 
where ErrorMessage is null or ErrorMessage="" 
group by itemid;




-------------------------women_clothing_item_attr_error-------------------------

CREATE TABLE if not exists elengjing.women_clothing_item_attr_error (
ItemID BIGINT,
AttrName STRING,
AttrValue STRING,
ErrorMessage STRING
)
CLUSTERED BY (ItemID) SORTED BY (ItemID) INTO 113 BUCKETS
STORED AS ORC;


INSERT INTO table elengjing.women_clothing_item_attr_error 
SELECT ta.ItemID,ta.AttrName,ta.AttrValue,ta.ErrorMessage
from t_elengjing.women_clothing_item_attr ta 
where ta.ErrorMessage is not null and ta.ErrorMessage!="";


-------------------------------------------------------------create PARTITION and  BUCKETS table---------------------------------------------------------------------------------------------------
/**/


CREATE TABLE if not exists elengjing.women_clothing_item(
ItemID BIGINT,
ItemName STRING,
ItemUrl STRING,
ListPrice DECIMAL(20,2),
DiscountPrice DECIMAL(20,2),
SalesQty BIGINT,
SalesAmt DECIMAL(20,2),
ShopID BIGINT,
AdChannel STRING,
BrandName STRING,
MainPicUrl STRING,
InStock STRING,
SKUList STRING,
CategoryID BIGINT,
ItemAttrDesc STRING,
Favorites STRING,
TotalComments STRING,
ListedDate STRING,
shopname STRING,
platform STRING,
categoryname STRING
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


-----------------------------------add new  itme to items----------------women_clothing_item-----------------------------------------------------------------------------------------------------------

INSERT INTO TABLE elengjing.women_clothing_item PARTITION(daterange)
SELECT 
a.ItemID,
a.ItemName,
a.ItemUrl,
a.ListPrice,
a.DiscountPrice,
a.SalesQty,
a.SalesAmt,
a.ShopID,
a.AdChannel,
a.BrandName,
a.MainPicUrl,
a.InStock,
a.SKUList,
a.CategoryID,
a.ItemAttrDesc,
a.Favorites,
a.TotalComments,
a.ListedDate,
b.shopname,
b.platform,
c.categoryname,
a.DateRange
FROM t_elengjing.women_clothing_item a
LEFT JOIN elengjing.shop b ON a.shopid=b.shopid
LEFT JOIN elengjing.category c ON a.CategoryID=c.categoryid;

----------------------------------------------------------END-------------------------------------------------------------------------------------------------------------


--------------------------------------全量 更新 women_clothing_item 中的shopname------------------------
---手动执行命令: beeline -u jdbc:hive2://mphd02:10000 -n hive -p hive1 -f /home/script/normal_servers/women_clothing/mp_update_item_shopname_all.sql
-------------------------------------------------------------------------------------------------------