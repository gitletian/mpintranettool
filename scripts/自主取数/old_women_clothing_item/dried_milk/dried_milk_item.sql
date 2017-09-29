-------------------create dried_milk_item ------------------------
--drop table if exists e_elengjing.dried_milk_item;
CREATE EXTERNAL TABLE if not exists e_elengjing.dried_milk_item(
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
CategoryID BIGINT,
Favorites STRING,
TotalComments STRING,
CategoryName STRING,
TotalOrders STRING,
SellerName STRING,
ShopRank STRING,
ShopLocation STRING,
ShopUrl STRING,
FansCount STRING,
ShopName STRING,
DSR STRING,
Platfrom STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY'\001';

----------------------------load data-----------------------------------------------
LOAD DATA LOCAL INPATH '${hivevar:CSV_DRIED_MILK_FILE}' OVERWRITE INTO TABLE e_elengjing.dried_milk_item;

----------------------------create orc  table --------------------------------------------------------

drop table if exists t_elengjing.dried_milk_item;
CREATE EXTERNAL TABLE if not exists t_elengjing.dried_milk_item(
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
CategoryID BIGINT,
Favorites STRING,
TotalComments STRING,
CategoryName STRING,
TotalOrders STRING,
SellerName STRING,
ShopRank STRING,
ShopLocation STRING,
ShopUrl STRING,
FansCount STRING,
ShopName STRING,
DSR STRING,
Platfrom STRING
)
STORED AS ORC;

----------------------------load data-----------------------------------------------
INSERT INTO table t_elengjing.dried_milk_item 
SELECT 
t.DateRange,
t.ItemID ,
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
t.CategoryID,
t.Favorites,
t.TotalComments,
t.CategoryName,
t.TotalOrders,
t.SellerName,
t.ShopRank,
t.ShopLocation,
t.ShopUrl,
t.FansCount,
t.ShopName,
t.DSR,
t.Platfrom
FROM 
(
select 
* 
from
(
select tw1.* ,ROW_NUMBER() OVER ( Partition By itemid,DateRange) AS rn
from e_elengjing.dried_milk_item tw1 
) tw where tw.rn=1
) t
left join 
elengjing.dried_milk_item t2 
on  t.ItemID = t2.ItemID and t.DateRange = t2.DateRange
where t2.ItemID is null and t2.DateRange is null;

------------------------------------------------process shop-----------------------------------------------------------------------

drop table if exists t_elengjing.shop;
CREATE TABLE if not exists t_elengjing.shop(
shopid BIGINT,
shopname STRING,
address STRING,
level INTEGER,
shop_url STRING,
favor BIGINT,
sellerid STRING,
nick STRING,
platform STRING,
dsr STRING
)
STORED AS ORC;


insert into table t_elengjing.shop
select 
a.ShopID,
a.ShopName,
"",
"",
a.ShopUrl,
a.Favorites,
"",
"",
a.Platfrom,
a.DSR
FROM 
(
select *
from
(
select s.*,ROW_NUMBER() OVER ( Partition By shopid)  rn
from t_elengjing.dried_milk_item s
) a
where a.rn=1
) t
left join elengjing.shop s1 on t.shopid = s1.shopid
where s1.shopid is null;


--------------------------------------sqoopl--shop------to pg tow table------mp_shop.sql---------------------------------------



--------------------------------------------------------------------------------------------------------------------
drop table if exists elengjing.dried_milk_item;
CREATE EXTERNAL TABLE if not exists elengjing.dried_milk_item(
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
CategoryID BIGINT,
Favorites STRING,
TotalComments STRING,
CategoryName STRING,
TotalOrders STRING,
SellerName STRING,
ShopRank STRING,
ShopLocation STRING,
ShopUrl STRING,
FansCount STRING,
ShopName STRING,
DSR STRING,
Platfrom STRING
)
PARTITIONED BY RANGE (DateRange DATE)(
PARTITION 2015_01 VALUES LESS THAN ('2015-01-31'),
PARTITION 2015_02 VALUES LESS THAN ('2015-02-28'),
PARTITION 2015_03 VALUES LESS THAN ('2015-03-31'),
PARTITION 2015_04 VALUES LESS THAN ('2015-04-30'),
PARTITION 2015_05 VALUES LESS THAN ('2015-05-31'),
PARTITION 2015_06 VALUES LESS THAN ('2015-06-30'),
PARTITION 2015_07 VALUES LESS THAN ('2015-07-31'),
PARTITION 2015_08 VALUES LESS THAN ('2015-08-31'),
PARTITION 2015_09 VALUES LESS THAN ('2015-09-30'),
PARTITION 2015_10 VALUES LESS THAN ('2015-10-31'),
PARTITION 2015_11 VALUES LESS THAN ('2015-11-30'),
PARTITION 2015_12 VALUES LESS THAN ('2015-12-31'),
PARTITION 2016_01 VALUES LESS THAN ('2016-01-31'),
PARTITION 2016_02 VALUES LESS THAN ('2016-02-29'),
PARTITION 2016_03 VALUES LESS THAN ('2016-03-31'),
PARTITION 2016_04 VALUES LESS THAN ('2016-04-30'),
PARTITION 2016_05 VALUES LESS THAN ('2016-05-31'),
PARTITION 2016_06 VALUES LESS THAN ('2016-06-30'),
PARTITION 2016_07 VALUES LESS THAN ('2016-07-31'),
PARTITION 2016_08 VALUES LESS THAN ('2016-08-31'),
PARTITION 2016_09 VALUES LESS THAN ('2016-09-30'),
PARTITION 2016_10 VALUES LESS THAN ('2016-10-31'),
PARTITION 2016_11 VALUES LESS THAN ('2016-11-30'),
PARTITION 2016_12 VALUES LESS THAN ('2016-12-31')
)
CLUSTERED BY (shopid) SORTED BY (categoryid) INTO 113 BUCKETS
STORED AS ORC;



INSERT INTO TABLE elengjing.dried_milk_item PARTITION(daterange)
select
t.ItemID ,
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
t.CategoryID,
t.Favorites,
t.TotalComments,
t.CategoryName,
t.TotalOrders,
t.SellerName,
t.ShopRank,
t.ShopLocation,
t.ShopUrl,
t.FansCount,
t.ShopName,
t.DSR,
t.Platfrom,
t.DateRange
from t_elengjing.dried_milk_item t;
------------------------------------END------------------------------------------------------------------































