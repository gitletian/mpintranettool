-- 数据拉去:wget --directory-prefix=/home/data/tmp/drug_item/  ftp://106.15.32.68/drug_item_2017_02_24.zip --ftp-user=user1 --ftp-password=Marcpointuser1
set mapred.fairscheduler.pool=poolHigh;
set ngmr.partition.automerge=true;
set ngmr.partition.mergesize.mb=200;
-- ls | xargs -i -t unzip {}
-- 1、数据入库
/**/
drop table if exists mpintranet.drug_item_load;
CREATE TABLE  if not exists mpintranet.drug_item_load(
DateRange  DATE,
ItemID  BIGINT,
ItemUrl  string,
ItemName  string,
ItemSubTitle  string,
MainPicUrl  string,
ItemAttrDesc  string,
ListPrice  string,
DiscountPrice  string,
UnitPrice  string,
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


LOAD DATA LOCAL INPATH '/home/data/tmp/drug_item/*.csv' OVERWRITE  INTO TABLE mpintranet.drug_item_load;


-- 2、对价格进行合并处理
drop table if exists mpintranet.drug_item_combine_price;
CREATE TABLE  if not exists mpintranet.drug_item_combine_price(
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


add file /home/guoyuanpei/mpintranet/zzqs/combine_price_a7.py;

insert into mpintranet.drug_item_combine_price
select
TRANSFORM (DateRange, ItemID, ItemName, ItemAttrDesc, ShopId, ShopName, PlatformId, CategoryId, SKUList, price_list, MonthlySalesQty, DiscountPrice, PlatformId)
USING "python combine_price_a7.py"
AS (DateRange, ItemID, ItemName, ItemAttrDesc, ShopId, ShopName, PlatformId, CategoryId, SKUList, MonthlySalesQty, DiscountPrice, error_info)
from
mpintranet.drug_item_load
;




-- 3.2 计算日库变

drop table if exists mpintranet.drug_item_stock_change;
CREATE TABLE  if not exists mpintranet.drug_item_stock_change(
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

add file /home/guoyuanpei/mpintranet/zzqs/day_stock_change7.py;


insert into mpintranet.drug_item_stock_change
select

TRANSFORM (p.ItemID, p.SKUList, y.SKUList, '', p.DateRange)
USING "python day_stock_change7.py"
AS (DateRange, ItemID, SKUList, sku_day_salesqty, sku_day_salesamt, sku_day_stock_change, is_has_months_change, error_info)

from
mpintranet.drug_item_combine_price p
left join mpintranet.drug_item_combine_price y
on p.itemid = y.itemid and date_sub(p.DateRange, 1) = y.DateRange
;




---  4、输出结果
select
l.*,
s.SKUList as new_SKUList,
s.sku_day_salesqty,
s.sku_day_salesamt,
s.sku_day_stock_change,
s.error_info
from
mpintranet.drug_item_load l
left join
mpintranet.drug_item_stock_change s
on
l.itemid = s.itemid and l.DateRange = s.DateRange
;