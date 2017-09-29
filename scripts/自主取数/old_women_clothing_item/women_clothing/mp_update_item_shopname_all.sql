--------------------------------------------------------------------------------------
----------                           全量 更新 women_clothing_item 中的shopname
----------1、更新shopname
----------
----------
--------------------------------------------------------------------------------------
set mapred.fairscheduler.pool=poolHigh;
set ngmr.partition.automerge=true;
set ngmr.partition.mergesize.mb=200;
----------------------------------------------------------item update shop -------------------------------------------------------------------------------------------------------------
drop table if exists elengjing.women_clothing_item_new;
CREATE TABLE if not exists elengjing.women_clothing_item_new(
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




INSERT INTO TABLE elengjing.women_clothing_item_new PARTITION(daterange)
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
a.categoryname,
a.DateRange
FROM elengjing.women_clothing_item a
LEFT JOIN elengjing.shop b ON a.shopid=b.shopid;

drop table if exists elengjing.women_clothing_item_bak;
use elengjing;
ALTER TABLE elengjing.women_clothing_item RENAME TO elengjing.women_clothing_item_bak;
ALTER TABLE elengjing.women_clothing_item_new RENAME TO elengjing.women_clothing_item;