--------------------------------------------------------------------------------------
----------                   elengjing 全量数据入库流程
----------
---------- wget --directory-prefix=/home/data/mjw_item_2017/  ftp://115.231.103.216/hy_item_07.zip --ftp-user=marcpoint --ftp-password=d7hN^AG:zj8v
--------------------------------------------------------------------------------------


--------------------------------------------------全量数据入库-------------------------------------------------------------

---------------------------------------------------------------------------- 一、 2016 & 2015 年数据入库--------------------------------------------------------------------------
------------ 1.1、 预备、旧 category 数据入库
drop table if exists extract.category;
CREATE TABLE if not exists extract.category(
     categoryid   bigint
    ,categoryname string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY'\t'
;

LOAD DATA LOCAL INPATH '/home/data/women_clothing_item/category.csv' overwrite INTO TABLE extract.category
;


drop table if exists elengjing.category;
CREATE TABLE if not exists elengjing.category(
     categoryid   bigint
    ,categoryname string
)
STORED AS ORC
;

insert into table elengjing.category
select * from extract.category
;




------------ 1.2、旧 mjw shop 数据入库

------ 1.2.1、数据入库
drop table if exists extract.shop;
CREATE TABLE if not exists extract.shop(
    shopid BIGINT,
    shopname STRING,
    address STRING,
    level int,
    shop_url STRING,
    favor BIGINT,
    sellerid STRING,
    nick STRING,
    platform STRING,
    dsr STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY'\001'
;

LOAD DATA LOCAL INPATH '/home/data/women_clothing_item/shop/*.csv' overwrite INTO TABLE extract.shop
;

------ 1.2.2、数据去重

drop table if exists elengjing.shop;
CREATE TABLE if not exists elengjing.shop(
    shopid BIGINT,
    shopname STRING,
    address STRING,
    level int,
    shop_url STRING,
    favor BIGINT,
    sellerid STRING,
    nick STRING,
    platform STRING,
    dsr STRING
)
STORED AS ORC
;


insert into table elengjing.shop
select
    a.shopid,
    a.shopname,
    a.address,
    a.level,
    a.shop_url,
    a.favor,
    a.sellerid,
    a.nick,
    case
        when a.platform = "淘宝" then 7001
        when a.platform = "天猫" then 7011
        else platform
    end as platform,
    a.dsr
from
(
select s.*,ROW_NUMBER() OVER ( Partition By shopid)  rn
from extract.shop s where s.shopid is not null and nvl(s.shopname, '') != ''
) a
where a.rn=1
;

------------ 1.3、旧 mjw item 数据入库

------ 1.3.1、数据入库

drop table if exists extract.women_clothing_item;
CREATE TABLE  if not exists extract.women_clothing_item(
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
FIELDS TERMINATED BY'\001'
;

LOAD DATA LOCAL INPATH '/var/data/women_clothing_item/mjw_item/*.bz2' OVERWRITE INTO TABLE extract.women_clothing_item;
;



------ 1.3.2、数据去重

drop table if exists transforms.women_clothing_item;
CREATE TABLE if not exists transforms.women_clothing_item (
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
STORED AS ORC
;



INSERT INTO table transforms.women_clothing_item
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
from
(
select tw1.* ,ROW_NUMBER() OVER ( Partition By DateRange, itemid) AS rn
from extract.women_clothing_item tw1
) t where t.rn=1
;

------ 1.3.3、转化skulist

drop table if exists transforms.item_skulist;
CREATE TABLE if not exists transforms.item_skulist (
    dateRange DATE,
    platformid STRING,
    salesQty BIGINT,
    salesAmt DECIMAL(20,2),
    stock BIGINT,
    skulist STRING,
    ItemID BIGINT,
    error_info STRING
)
STORED AS ORC
;

add file /home/script/normal_servers/serverudf/elengjing/skulis_combine_9.py;

insert into table transforms.item_skulist
SELECT
  TRANSFORM (dateRange, "tmall", salesQty, salesAmt, InStock, skulist, "", ItemID)
  USING 'python skulis_combine_9.py'
  AS (dateRange, platformid, salesQty, salesAmt, stock, export_skulist, ItemID, error_info)
FROM transforms.women_clothing_item
;

------ 1.3.4、join 产生全量 新字典表

drop table if exists elengjing_base.women_clothing_item_new_dict;
CREATE TABLE  if not exists elengjing_base.women_clothing_item_new_dict(
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
ShopUrl  string,
Categoryname  string,
listeddate  string,
DateRange DATE
)
PARTITIONED BY (month string)
STORED AS ORC;



insert into elengjing_base.women_clothing_item_new_dict PARTITION(month)
select
 t1.itemid
,t1.itemurl
,t1.itemname
,''
,t1.mainpicurl
,t1.itemattrdesc
,t1.listprice
,t1.discountprice
,''
,''
,t1.salesqty
,t1.salesamt
,t1.instock
,t4.skulist
,t1.favorites
,t1.totalcomments
,t1.brandname
,t1.categoryid
,t1.shopid
,t1.adchannel
,t3.platform
,''
,''
,''
,t3.shopname
,t3.Shop_Url
,t2.categoryname
,t1.listeddate
,t1.daterange
,substr(t1.daterange, 1, 7)
from
transforms.women_clothing_item t1
left join
elengjing.category t2
on t1.categoryid = t2.categoryid
left join
elengjing.shop t3
on t1.shopid = t3.shopid
left JOIN
transforms.item_skulist t4
on t1.ItemID = t4.ItemID and t1.dateRange = t4.dateRange
;



--------------------------------------------------------------------------------------------- 二、2017 年数据入库--------------------------------------------------------------------------
------------ 2.1、 自主抓取数据入库
------ 2.1.1、数据入库
drop table if exists extract.women_clothing_item_load;
CREATE TABLE  if not exists extract.women_clothing_item_load(
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


LOAD DATA LOCAL INPATH '/var/data/women_clothing_item/item/*.bz2' OVERWRITE  INTO TABLE extract.women_clothing_item_load;


------ 2.1.2、去重过滤
drop table if exists extract.women_clothing_item_load_unique;
CREATE TABLE  if not exists extract.women_clothing_item_load_unique(
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
STORED AS ORC;


insert into extract.women_clothing_item_load_unique
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
from extract.women_clothing_item_load a
where
PlatformId in (7001, 7011)
and nvl(shopid, '') != ''
and nvl(CategoryId, 11111111111) in (1622,1623,1624,1629,1636,162103,162104,162105,162116,162201,162205,162401,162402,162403,162404,162701,162702,162703,50000671,50000697,50000852,50005065,50003509,50003510,50003511,50007068,50008897,50008898,50008899,50008900,50008901,50008903,50008904,50008905,50008906,50010850,50011277,50011404,50011411,50011412,50011413,50013194,50013196,50022566,50026651,121412004,121434004,123216004)
;



------------ 2.2、mjw 2017年数据入库
------ 2.2.1、mjw 数据入库

drop table if exists extract.mjw_item;
CREATE TABLE if not exists extract.mjw_item(
    dateRange DATE,
    ItemID BIGINT,
    platform STRING,
    salesQty BIGINT,
    salesAmt DECIMAL(20,2),
    stock BIGINT,
    skulist STRING,
    skudesc STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY'\001'
;

LOAD DATA LOCAL INPATH '/home/data/*.bz2' overwrite INTO TABLE extract.mjw_item
;


drop table if exists transforms.mjw_item;
CREATE TABLE if not exists transforms.mjw_item (
    dateRange DATE,
    ItemID BIGINT,
    platform STRING,
    salesQty BIGINT,
    salesAmt DECIMAL(20,2),
    stock BIGINT,
    skulist STRING,
    skudesc STRING
)
STORED AS ORC
;

insert into table transforms.mjw_item
select * from extract.mjw_item;

------ 2.2.2、去重
drop table if exists transforms.mjw_item_unique;
CREATE TABLE if not exists transforms.mjw_item_unique (
    dateRange DATE,
    ItemID BIGINT,
    platform STRING,
    salesQty BIGINT,
    salesAmt DECIMAL(20,2),
    stock BIGINT,
    skulist STRING,
    skudesc STRING
)
STORED AS ORC
;

insert into table transforms.mjw_item_unique
select
 daterange
,itemid
,platform
,salesqty
,salesamt
,stock
,skulist
,skudesc
from
(
select tw1.* ,ROW_NUMBER() OVER ( Partition By DateRange, itemid) AS rn
from transforms.mjw_item tw1
) t where t.rn=1
;


------ 2.2.3、合并skulist
drop table if exists transforms.item_skulist;
CREATE TABLE if not exists transforms.item_skulist (
    dateRange DATE,
    platformid STRING,
    salesQty BIGINT,
    salesAmt DECIMAL(20,2),
    stock BIGINT,
    skulist STRING,
    ItemID BIGINT,
    error_info STRING
)
STORED AS ORC
;

add file /home/script/normal_servers/serverudf/elengjing/skulis_combine_9.py;

insert into table transforms.item_skulist
SELECT
  TRANSFORM (dateRange, platform, salesQty, salesAmt, stock, skulist, skudesc, ItemID)
  USING 'python skulis_combine_9.py'
  AS (dateRange, platformid, salesQty, salesAmt, stock, export_skulist, ItemID, error_info)
FROM transforms.mjw_item_unique
;

------------ 2.3、 mjw 数据和自主抓取的数据合并

------ 2.3.1、共同数据合并

drop table if exists transforms.women_clothing_item_new_dict_2017;
CREATE TABLE  if not exists transforms.women_clothing_item_new_dict_2017(
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
ShopUrl  string,
Categoryname  string,
listeddate  string,
DateRange DATE
)
STORED AS ORC;



insert into transforms.women_clothing_item_new_dict_2017
select
 t1.itemid
,t1.itemurl
,t1.itemname
,t1.ItemSubTitle
,t1.mainpicurl
,t1.itemattrdesc
,t1.listprice
,round(t2.salesAmt / t2.salesqty, 2)
,t1.UnitPrice
,t1.Unit
,t2.salesqty
,t2.salesamt
,t2.stock
,t2.skulist
,t1.favorites
,t1.totalcomments
,t1.brandname
,t1.categoryid
,t1.shopid
,t1.adchannel
,t2.PlatformId
,t1.keyword
,t1.MonthlySalesQty
,t1.TotalSalesQty
,t4.shopname
,t4.shop_url
,t3.categoryname
,t1.listeddate
,t2.daterange
from
transforms.item_skulist t2
inner JOIN
extract.women_clothing_item_load_unique t1
on t1.itemid = t2.itemid and t1.daterange = t2.daterange
left join
elengjing.category t3
on t1.categoryid = t3.categoryid
left join
elengjing.shop t4
on t1.shopid = t4.shopid
;


------ 2.3.2、合并
insert into elengjing_base.women_clothing_item_new_dict PARTITION(month)
select *, substr(daterange, 1, 7) from transforms.women_clothing_item_new_dict_2017
;

------ 2.3.3、修复合并(自主抓取未抓到的)

-- 2.3.3.1 、获取为抓取到的item
drop table if exists transforms.mjw_item_skulist_other;
CREATE TABLE if not exists transforms.mjw_item_skulist_other (
    dateRange STRING,
    platformid STRING,
    salesQty STRING,
    salesAmt STRING,
    stock STRING,
    skulist STRING,
    ItemID STRING,
    error_info STRING
)
STORED AS ORC
;


insert into table transforms.mjw_item_skulist_other
select
 t1.daterange
,t1.platformid
,t1.salesqty
,t1.salesamt
,t1.stock
,t1.skulist
,t1.itemid
,t1.error_info
from
transforms.item_skulist t1
left JOIN
extract.women_clothing_item_load_unique t2
on t1.itemid = t2.itemid and t1.daterange = t2.daterange
where t2.itemid is null
;


-- 2.3.3.2 、获取当前数据去重后的数据


drop table if exists transforms.women_clothing_item_new_dict_unique_desc;
CREATE TABLE  if not exists transforms.women_clothing_item_new_dict_unique_desc(
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
ShopUrl  string,
Categoryname  string,
listeddate  string,
DateRange DATE
)
STORED AS ORC;


insert into transforms.women_clothing_item_new_dict_unique_desc
select
 itemid
,itemurl
,itemname
,itemsubtitle
,mainpicurl
,itemattrdesc
,listprice
,discountprice
,unitprice
,unit
,salesqty
,salesamt
,instock
,skulist
,favorites
,totalcomments
,brandname
,categoryid
,shopid
,adchannel
,platformid
,keyword
,monthlysalesqty
,totalsalesqty
,shopname
,shopurl
,categoryname
,listeddate
,daterange
FROM
(
select tw1.* ,ROW_NUMBER() OVER ( Partition By itemid order by DateRange desc) AS rn
from elengjing_base.women_clothing_item_new_dict tw1
) t where t.rn=1
;


-- 2.3.3.3 、修复数据


drop table if exists transforms.women_clothing_item_new_dict_repaire_2017;
CREATE TABLE  if not exists transforms.women_clothing_item_new_dict_repaire_2017(
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
ShopUrl  string,
Categoryname  string,
listeddate  string,
DateRange DATE
)
STORED AS ORC;



insert into transforms.women_clothing_item_new_dict_repaire_2017
select
 t1.itemid
,t1.itemurl
,t1.itemname
,t1.ItemSubTitle
,t1.mainpicurl
,''
,t1.listprice
,round(t2.salesAmt / t2.salesqty, 2)
,t1.UnitPrice
,t1.Unit
,t2.salesqty
,t2.salesamt
,t2.stock
,t2.skulist
,t1.favorites
,t1.totalcomments
,t1.brandname
,t1.categoryid
,t1.shopid
,t1.adchannel
,t2.PlatformId
,t1.keyword
,''
,''
,t4.shopname
,t4.shop_url
,t3.categoryname
,t1.listeddate
,t2.daterange
FROM
transforms.women_clothing_item_new_dict_unique_desc t1
inner JOIN
transforms.mjw_item_skulist_other t2
on t1.itemid = t2.itemid
left join
elengjing.category t3
on t1.categoryid = t3.categoryid
left join
elengjing.shop t4
on t1.shopid = t4.shopid
;

-- 2.3.3.3 、合并修复

insert into elengjing_base.women_clothing_item_new_dict PARTITION(month)
select *, substr(daterange, 1, 7) from transforms.women_clothing_item_new_dict_repaire_2017
;



-------------- 2.4、 补充抓取数据进行合并
------ 2.4.1、获取需要补充修复的 数据

/**/
drop table if exists transforms.zzzq_no_item;
CREATE TABLE if not exists transforms.zzzq_no_item like transforms.item_skulist_201707;

with noitem as (
    select
     t2.itemid
    FROM
    transforms.mjw_item_skulist_other_201707 t2
    left JOIN
    transforms.women_clothing_item_new_dict_unique_desc t1
    on t1.itemid = t2.itemid
    where t1.itemid is null
    group by t2.itemid
)

insert into transforms.zzzq_no_item
select
t1.*
from
transforms.item_skulist_201707 t1
inner join
noitem t2
on t1.itemid = t2.itemid
;

------ 2.4.2、导出需要不抓的数据

insert overwrite local directory "/home/data/tmp/supplement_item_201707"
row format delimited
fields terminated by "\t"
select
 itemid
,max(PlatformId) as PlatformId
FROM
transforms.zzzq_no_item
group by itemid
;


------ 2.4.3、合并不抓数据
drop table if exists transforms.women_clothing_item_new_dict_zzzq_r;
CREATE TABLE  if not exists transforms.women_clothing_item_new_dict_zzzq_r(
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
ShopUrl  string,
Categoryname  string,
listeddate  string,
DateRange DATE
)
STORED AS ORC;



insert into transforms.women_clothing_item_new_dict_zzzq_r
select
 t1.itemid
,t1.itemurl
,t1.itemname
,t1.ItemSubTitle
,t1.mainpicurl
,t1.itemattrdesc
,t1.listprice
,round(t2.salesAmt / t2.salesqty, 2)
,t1.UnitPrice
,t1.Unit
,t2.salesqty
,t2.salesamt
,t2.stock
,t2.skulist
,t1.favorites
,t1.totalcomments
,t1.brandname
,t1.categoryid
,t1.shopid
,t1.adchannel
,t2.PlatformId
,t1.keyword
,t1.MonthlySalesQty
,t1.TotalSalesQty
,t3.shopname
,t3.shop_url
,t4.categoryname
,t1.listeddate
,t2.daterange
from
transforms.zzzq_no_item t2
inner join
extract.women_clothing_item_load_unique_r t1
on t1.itemid = t2.itemid
left join
elengjing.shop t3
on t1.shopid = t3.shopid
left join
elengjing.category t4
on t1.categoryid = t4.categoryid
;


insert into table elengjing_base.women_clothing_item_new_dict PARTITION(month)
select *, substr(daterange, 1, 7) from transforms.women_clothing_item_new_dict_zzzq_r
;

------------------------------------------------------------------------------------------- 三、整体数据进行 提取--------------------------------------------------------------------------


------------ 3.1、 对数据进行打标签

------ 3.1.1、提取 desc 表
drop table if exists transforms.women_clothing_item_description;
CREATE table  if not exists transforms.women_clothing_item_description(
 itemid                bigint
,itemname              string
,itemattrdesc          string
,categoryid            bigint
,shopid                bigint
,daterange             date
)
stored as orc
;


insert into table transforms.women_clothing_item_description
select
 t.itemid
,t.itemname
,t.itemattrdesc
,t.categoryid
,t.shopid
,t.daterange
from
(
select tw1.* ,ROW_NUMBER() OVER (Partition By tw1.itemid ORDER BY tw1.DateRange  desc) AS rn
from elengjing_base.women_clothing_item_new_dict tw1 where nvl(tw1.itemattrdesc, '') != ''
) t where t.rn=1
;


------ 3.1.1、补充修复 description 合并
/*




drop table if exists transforms.women_clothing_item_description_r;
CREATE table  if not exists transforms.women_clothing_item_description_r(
 itemid                bigint
,itemname              string
,itemattrdesc          string
,categoryid            bigint
,shopid                bigint
,daterange             date
)
stored as orc
;



insert into table transforms.women_clothing_item_description_r
select
 t1.itemid
,t1.itemname
,t1.itemattrdesc
,t1.categoryid
,t1.shopid
,t1.daterange
from
extract.women_clothing_item_load_unique_r t1
left join
transforms.women_clothing_item_description t2
on t1.itemid = t2.itemid
where t2.itemid is null
;


insert into table transforms.women_clothing_item_description
select * from transforms.women_clothing_item_description_r;



select
count(1)
from
(
select tw1.* ,ROW_NUMBER() OVER ( Partition By itemid order by DateRange desc) AS rn
from transforms.women_clothing_item_description tw1
) t where t.rn=1
;



*/


------ 3.1.2、进行打标签

drop table if exists elengjing.women_clothing_item_attr;
CREATE TABLE if not exists elengjing.women_clothing_item_attr(
 itemid BIGINT
,shopid BIGINT
,categoryid BIGINT
,chongrongliang    String
,hanrongliang    String
,tuan    String
,tuanwenhua    String
,tianchongwu    String
,gongyi    String
,langxing    String
,chengfenhanliang    String
,baixing    String
,caizhichengfen    String
,kuanshi    String
,banxing    String
,lifubaixing    String
,yaoxing    String
,menyijin    String
,xiuxing    String
,xiuchang    String
,qunxing    String
,qunchang    String
,kuxing    String
,kuchang    String
,jinxing    String
,liliao    String
,mianliao    String
,lingzi    String
,fengge    String
,shiyongnianling String
,errormessage STRING
)
STORED AS ORC
;



-- 3.1.2.1 、对2017年之前的数据进行打标签

add jar /home/script/normal_servers/serverudf/elengjing/Tagged_test_9.jar;

drop temporary function if exists Tagged_extend;
create temporary function Tagged_extend as 'com.marcpoint.elengjing_extend.Tagged_extend_1';


set elengjing.var.separator_conf=old;
insert into elengjing.women_clothing_item_attr
select
Tagged_extend(tw.ItemID,tw.categoryid,tw.shopid,tw.itemname,regexp_replace(tw.ItemAttrDesc, '\t', ' '))  as (itemid, shopid, categoryid, chongrongliang, hanrongliang, tuan, tuanwenhua, tianchongwu, gongyi, langxing, chengfenhanliang, baixing, caizhichengfen, kuanshi, banxing, lifubaixing, yaoxing, menyijin, xiuxing, xiuchang, qunxing, qunchang, kuxing, kuchang, jinxing, liliao, mianliao, lingzi, fengge, shiyongnianling, errormessage)
from
transforms.women_clothing_item_description tw where tw.daterange < '2017-01-01'
;


-- 3.1.2.2 、对2017年之后的数据进行打标签

set elengjing.var.separator_conf=new;
insert into elengjing.women_clothing_item_attr
select
Tagged_extend(tw.ItemID,tw.categoryid,tw.shopid,tw.itemname,regexp_replace(tw.ItemAttrDesc, '\t', ' '))  as (itemid, shopid, categoryid, chongrongliang, hanrongliang, tuan, tuanwenhua, tianchongwu, gongyi, langxing, chengfenhanliang, baixing, caizhichengfen, kuanshi, banxing, lifubaixing, yaoxing, menyijin, xiuxing, xiuchang, qunxing, qunchang, kuxing, kuchang, jinxing, liliao, mianliao, lingzi, fengge, shiyongnianling, errormessage)
from
transforms.women_clothing_item_description tw where tw.daterange >= '2017-01-01'
;



/*
drop table if exists transforms.women_clothing_item_attr;
CREATE TABLE if not exists transforms.women_clothing_item_attr(
itemid BIGINT,
shipid BIGINT,
categoryid BIGINT,
attrname STRING,
attrvalue STRING,
errormessage STRING
)
CLUSTERED BY (ItemID) SORTED BY (ItemID) INTO 113 BUCKETS
STORED AS ORC;


add jar /home/script/normal_servers/serverudf/elengjing/Tagged_test_9.jar;
drop temporary function if exists Tagged_o;
create temporary function Tagged_o as 'com.marcpoint.elengjing.Tagged_o';


set elengjing.var.separator_conf=old;
insert into transforms.women_clothing_item_attr
select
Tagged_o(tw.ItemID, tw.categoryid, tw.shopID,tw.ItemName,tw.ItemAttrDesc)  as (itemid, shopID, categoryid, attrname,attrvalue,errormessage)
from
transforms.women_clothing_item_description tw where tw.daterange < '2017-01-01'
;


set elengjing.var.separator_conf=new;
insert into transforms.women_clothing_item_attr
select
Tagged_o(tw.ItemID, tw.categoryid, tw.shopID,tw.ItemName,tw.ItemAttrDesc)  as (itemid, shopID, categoryid, attrname,attrvalue,errormessage)
from
transforms.women_clothing_item_description tw where tw.daterange >= '2017-01-01'
;

*/


------------- 3.2、 获取 stockAdd、stockNew 和 其他指标信息
/*



------ 3.2.1、计算 stockAdd、stockNew (需计算2016-07-01之后每天的)

-- 3.2.1.1、提取stock 信息
drop table if exists transforms.women_clothing_item_instock;
CREATE table  if not exists transforms.women_clothing_item_instock(
 itemid                bigint
,instock               bigint
,DateRange DATE
)
stored as orc
;

insert into table transforms.women_clothing_item_instock
select itemid, instock, daterange from elengjing_base.women_clothing_item_new_dict where daterange < "2017-01-01"
;

insert into table transforms.women_clothing_item_instock
select itemid, stock, daterange from transforms.mjw_item_unique
;



-- 3.2.1.2、计算 stockAdd、stockNew

drop table if exists transforms.item_stock;
create table  if not exists transforms.item_stock(
 itemID                BIGINT
,daterange             date
,stockAdd              BIGINT
,stockNew              BIGINT

)
stored as orc
;


with item_add as (
    select itemid, daterange, instock from transforms.women_clothing_item_instock where daterange = '2016-07-01' and nvl(instock, '') != ''
),
item_unique_desc as (
    select t.itemid, t.instock
    from
    (
        select tw1.itemid, tw1.instock, ROW_NUMBER() OVER (Partition By tw1.itemid ORDER BY tw1.DateRange  desc) AS rn
        from transforms.women_clothing_item_instock tw1 where tw1.daterange < '2016-07-01' and nvl(tw1.instock, '') != ''
    ) t where t.rn=1
)

insert into table transforms.item_stock
select
    t1.itemid,
    t1.daterange,
    if(t2.itemid is not null and if(nvl(t1.instock, '') != '', t1.instock, 0) > if(nvl(t2.instock, '') != '', t2.instock, 0), if(nvl(t1.instock, '') != '', t1.instock, 0) - if(nvl(t2.instock, '') != '', t2.instock, 0), 0) as stockAdd,
    if(t2.itemid is null, t1.instock, 0) as stockNew
FROM
item_add t1
left JOIN
item_unique_desc t2
on t1.itemid = t2.itemid
;


------ 3.2.2、计算 linkSalesqty、 yearSalesqty、linkSalesamt、yearSalesamt (只需要初始化最近三十天的)

drop table if exists transforms.relative_kpi;
create table  if not exists transforms.relative_kpi(
 itemID                BIGINT
,daterange             date
,salesqty              bigint
,salesamt              decimal(20,2)

,linkSalesqty          bigint
,yearSalesqty          bigint
,linkSalesamt          decimal(20,2)
,yearSalesamt          decimal(20,2)

)
stored as orc
;

-- set mapred.reduce.tasks = 3000;

with base as (
select
itemid,
daterange,
salesqty,
salesamt
from
elengjing_base.women_clothing_item_new_dict where daterange > '2017-05-20'
)

insert into table transforms.relative_kpi
select
b.itemid,
b.daterange,
b.salesqty,
b.salesamt,

nvl(l.salesqty, 0),
nvl(y.salesqty, 0),
nvl(l.salesamt, 0),
nvl(y.salesamt, 0)
from
base b
left join
(select itemid, daterange, salesqty, salesamt from elengjing_base.women_clothing_item_new_dict where daterange > '2017-04-20') l
on b.itemid = l.itemid and b.daterange = date_sub(l.daterange, -1)
left join
(select itemid, daterange, salesqty, salesamt from elengjing_base.women_clothing_item_new_dict where daterange between '2016-05-01' and '2016-07-20') y
on b.itemid = y.itemid and b.daterange = add_months(y.daterange, 12)
;

*/







------------ 3.3.1、 计算首次上架时间 生成 transforms.item_listedDate


drop table if exists transforms.item_listedDate;
create table  if not exists transforms.item_listedDate(
 itemID                BIGINT,
 daterange             date
)
stored as orc
;

insert into table transforms.item_listedDate
select
itemid,
min(daterange) as daterange
FROM
elengjing_base.women_clothing_item_new_dict
-- where nvl(stock, 0) != 0
group by itemid
;


------------ 3.3、 合并 生成 women_clothing_item


drop table if exists elengjing.women_clothing_item;
create table  if not exists elengjing.women_clothing_item(
 itemID                BIGINT
,itemname              string
,ItemUrl               string
,MainPicUrl            string
,ItemAttrDesc          string
,dateRange             date
,salesqty              BIGINT
,salesamt              decimal(20,2)
,discountPrice         decimal(20,2)
,similarPrice          BIGINT
,shopID                BIGINT
,shopName              string
,ShopUrl               string
,categoryID            BIGINT
,categoryName          string
,platformID            int
,listedDate            string
,favorites             BIGINT

,stock                 BIGINT
,discount              BIGINT

,stockAdd              BIGINT
,stockNew              BIGINT

,linkSalesqty          BIGINT
,yearSalesqty          BIGINT
,linkSalesamt          decimal(20,2)
,yearSalesamt          decimal(20,2)
)
PARTITIONED BY (month string)
stored as orc
;


set mapred.reduce.tasks = 3000;

insert into table elengjing.women_clothing_item PARTITION(month)
select
t1.itemid,
t1.itemname,
t1.ItemUrl,
t1.MainPicUrl,
t1.ItemAttrDesc,
t1.daterange,
t1.salesqty,
t1.salesamt,
t1.discountPrice,
if(t1.discountPrice % 5 >= 2.5, floor(t1.discountPrice / 5) + 1, floor(t1.discountPrice / 5)) * 5,
t1.shopID,
t1.shopName,
t1.ShopUrl,
t1.categoryID,
t1.categoryName,
t1.platformID,
if(t4.itemid is not null, t4.listedDate, cast(t1.daterange as string)),
t1.favorites,

t1.instock,
if(t1.listprice is null, 10, if(nvl(floor(t1.discountprice * 1.0 / t1.listprice * 10), 10) > 10, 10, nvl(floor(t1.discountprice * 1.0 / t1.listprice * 10), 10))),

nvl(t2.stockAdd, 0),
nvl(t2.stockNew, 0),

nvl(t3.linkSalesqty, 0),
nvl(t3.yearSalesqty, 0),
nvl(t3.linkSalesamt, 0),
nvl(t3.yearSalesamt, 0),
substr(t1.daterange, 1, 7)
from
(select * from elengjing_base.women_clothing_item_new_dict where daterange < '2016-07-01') t1
left join
transforms.item_stock t2
on t1.itemid = t2.itemid and t1.daterange = t2.daterange
left join
transforms.relative_kpi t3
on t1.itemid = t3.itemid and t1.daterange = t3.daterange
left JOIN
transforms.item_listedDate t4
on t1.itemid = t4.itemid
;


------------ 3.4、 生成 women_clothing_item_unique 表

------ 3.4.1、去重并且求取 首次上架时间

drop table if exists elengjing.women_clothing_item_unique_desc;
create table  if not exists elengjing.women_clothing_item_unique_desc(
 itemID                BIGINT
,itemname              string
,ItemUrl               string
,MainPicUrl            string
,dateRange             date
,salesqty              BIGINT
,salesamt              decimal(20,2)
,discountPrice         decimal(20,2)
,similarPrice          BIGINT
,shopID                BIGINT
,shopName              string
,ShopUrl               string
,categoryID            BIGINT
,categoryName          string
,platformID            int
,listedDate            string
,favorites             BIGINT

,stock                 BIGINT
,discount              BIGINT

,stockAdd              BIGINT
,stockNew              BIGINT

,linkSalesqty          BIGINT
,yearSalesqty          BIGINT
,linkSalesamt          decimal(20,2)
,yearSalesamt          decimal(20,2)
)
stored as orc
;

insert into table elengjing.women_clothing_item_unique_desc
select
 t.itemid
,t.itemname
,t.itemurl
,t.mainpicurl
,t.daterange
,t.salesqty
,t.salesamt
,t.discountprice
,t.similarprice
,t.shopid
,t.shopname
,t.shopurl
,t.categoryid
,t.categoryname
,t.platformid
,t.listeddate
,t.favorites
,t.stock
,t.discount
,t.stockadd
,t.stocknew
,t.linksalesqty
,t.yearsalesqty
,t.linksalesamt
,t.yearsalesamt
from
(
    select tw1.* ,ROW_NUMBER() OVER (Partition By tw1.itemid ORDER BY tw1.DateRange  desc) as rn
    from elengjing.women_clothing_item tw1
) t where t.rn=1
;


------ 3.4.2、计算kpi 汇总

drop table if exists transforms.sum_salesqty;
CREATE table  if not exists transforms.sum_salesqty(
    itemid              bigint,

    towWeekSalesqty     bigint,
    monthSalesqty       bigint,
    linkSalesqty        bigint,
    yearSalesqty        bigint,

    towWeekSalesamt     decimal(20,2),
    monthSalesamt       decimal(20,2),
    linkSalesamt        decimal(20,2),
    yearSalesamt        decimal(20,2)
)
CLUSTERED BY (itemid)  INTO 113 BUCKETS
stored as orc
;

set daterange=2017-10-11;

insert into table transforms.sum_salesqty
select
    t1.itemid,
    sum(if(datediff('${hiveconf:daterange}', t1.daterange) between 0 and 13, nvl(t1.salesqty, 0), 0)) as tow_week_salesqty,
    sum(if(datediff('${hiveconf:daterange}', t1.daterange) between 0 and 29, nvl(t1.salesqty, 0), 0)) as month_salesqty,

    sum(if(datediff(date_sub('${hiveconf:daterange}', 30), t1.daterange) between 0 and 29, nvl(t1.salesqty, 0), 0)) as link_salesqty,
    sum(if(datediff(add_months('${hiveconf:daterange}', -12), t1.daterange) between 0 and 29, nvl(t1.salesqty, 0), 0)) as year_salesqty,

    sum(if(datediff('${hiveconf:daterange}', t1.daterange) between 0 and 13, nvl(t1.salesamt, 0), 0)) as tow_week_salesamt,
    sum(if(datediff('${hiveconf:daterange}', t1.daterange) between 0 and 29, nvl(t1.salesamt, 0), 0)) as month_salesamt,

    sum(if(datediff(date_sub('${hiveconf:daterange}', 30), t1.daterange) between 0 and 29, nvl(t1.salesamt, 0), 0)) as link_salesamt,
    sum(if(datediff(add_months('${hiveconf:daterange}', -12), t1.daterange) between 0 and 29, nvl(t1.salesamt, 0), 0)) as year_salesamt

FROM
elengjing.women_clothing_item t1
group by t1.itemid
;


------ 3.4.3、合并生成 women_clothing_item_unique

drop table if exists elengjing.women_clothing_item_unique;
CREATE table  if not exists elengjing.women_clothing_item_unique(
 itemid                bigint
,itemname              string
,daterange             date
,itemurl               string
,itemsubtitle          string
,mainpicurl            string
,listprice             decimal(20,2)
,discountprice         bigint
,salesqty              bigint
,salesamt              decimal(20,2)
,favorites             bigint
,stock                 bigint
,categoryid            bigint
,categoryname          string
,shopid                bigint
,shopname              string
,shopurl               string
,listeddate            string
,platformid            int

,towWeekSalesqty       bigint
,monthSalesqty         bigint
,shopsalesqty          bigint
,linkSalesqty          bigint
,yearSalesqty          bigint

,towWeekSalesamt       decimal(20,2)
,monthSalesamt         decimal(20,2)
,linkSalesamt          decimal(20,2)
,yearSalesamt          decimal(20,2)


,dailyPrice            decimal(20, 2)
,eventPrice            decimal(20, 2)
,dailyPriceRatio       decimal(20, 2)
,eventPriceRatio       decimal(20, 2)

)
stored as orc
;




insert into table elengjing.women_clothing_item_unique
select
 t1.itemid
,t1.itemname
,t1.daterange
,t1.itemurl
,''
,t1.mainpicurl
,''
,int(if(t1.discountprice > 1000000, 1000000, t1.discountprice))
,t1.salesqty
,t1.salesamt
,t1.favorites
,t1.stock
,t1.categoryid
,t1.categoryname
,t1.shopid
,t1.shopname
,t1.shopurl
,t1.listeddate
,t1.platformid


,s.towWeekSalesqty
,s.monthSalesqty
,sum(nvl(s.monthSalesqty, 0)) over (Partition By t1.shopid)
,s.linkSalesqty
,s.yearSalesqty

,s.towWeekSalesamt
,s.monthSalesamt
,s.linkSalesamt
,s.yearSalesamt

,d.price
,e.price
,int(abs(rand() - 0.5))
,int(abs(rand() - 0.7))

from
elengjing.women_clothing_item_unique_desc t1
left JOIN
transforms.sum_salesqty s
on t1.itemid = s.itemid
left join
elengjing_price.predict_event e
on t1.itemid = e.itemid
left join
elengjing_price.predict_daily d
on t1.itemid = d.itemid
;


------------------------------------------------------------------------------------------- 四、数据导出到 pg  及 创建kylin视图--------------------------------------------------------------------------

------------ 4.1、 导出 women_clothing_item

------ 4.1.1、生成 women_clothing_item 导出表

drop table if exists transforms.export_women_clothing_item;
create table  if not exists transforms.export_women_clothing_item(
 itemID                BIGINT
,itemname              string
,dateRange             date
,salesqty              BIGINT
,salesamt              decimal(20,2)
,discountPrice         decimal(20,2)
,similarPrice          BIGINT
,shopID                BIGINT
,shopName              string
,categoryID            BIGINT
,categoryName          string
,platformID            BIGINT
,listedDate            string
,favorites             BIGINT

,stock                 BIGINT
,discount              BIGINT

,stockAdd              BIGINT
,stockNew              BIGINT

,linkSalesqty          BIGINT
,yearSalesqty          BIGINT
,linkSalesamt          decimal(20,2)
,yearSalesamt          decimal(20,2)
)
;


insert into table transforms.export_women_clothing_item
select
 itemid
,itemname
,daterange
,salesqty
,salesamt
,discountprice
,similarprice
,shopid
,shopname
,categoryid
,categoryname
,platformid
,listeddate
,favorites
,stock
,discount
,stockadd
,stocknew
,linksalesqty
,yearsalesqty
,linksalesamt
,yearsalesamt
from
elengjing.women_clothing_item
where daterange > '2017-08-08'
;

------ 4.1.2、导出的 sqoop

export
--connect
jdbc:postgresql://192.168.110.11:5432/elengjing
--username
elengjing
--password
Marcpoint2016
--call
save_women_clothing_item
--num-mappers
50
--input-fields-terminated-by
'\001'
--input-null-string
"\\N"
--input-null-non-string
"\\N"
--export-dir
hdfs://nameservice1/user/hive/warehouse/transforms.db/export_women_clothing_item/



sqoop --options-file /home/script/normal_servers/script/women_clothing_item_export.txt


nohup sqoop --options-file /home/script/tmp/sqoop.txt

------ 4.1.3、pg 的数据表


-- ----------------------------
DROP TABLE IF EXISTS "public"."women_clothing_item_new";
CREATE TABLE "public"."women_clothing_item_new" (
	"itemid" int8 NOT NULL,
	"itemname" varchar COLLATE "default",
	"daterange" date,
	"salesqty" int8,
	"salesamt" float8,
	"discountprice" numeric,
	"similarprice" int8,
	"shopid" int8,
	"shopname" varchar COLLATE "default",
	"categoryid" int8,
	"categoryname" varchar COLLATE "default",
	"platformid" int4,
	"listeddate" date,
	"favorites" int4,
	"stock" int8,
	"discount" int4,
	"stockadd" int8,
	"stocknew" int8,
	"linksalesqty" int8,
	"yearsalesqty" int8,
	"linksalesamt" float8,
	"yearsalesamt" float8
)
WITH (OIDS=FALSE);
ALTER TABLE "public"."women_clothing_item_new" OWNER TO "elengjing";

-- ----------------------------
--  Primary key structure for table women_clothing_item
-- ----------------------------
ALTER TABLE "public"."women_clothing_item_new" ADD PRIMARY KEY ("itemid", "daterange") NOT DEFERRABLE INITIALLY IMMEDIATE;


------ 4.1.4、pg 表的 索引

CREATE INDEX shopid ON women_clothing_item_new (shopid);


------ 4.1.5、pg 的saveorupdate

CREATE OR REPLACE FUNCTION "public"."save_women_clothing_item"(IN in_itemid int8, IN in_itemname varchar, IN in_daterange date, IN in_salesqty int8, IN in_salesamt float8, IN in_discountprice numeric, IN in_similarprice int8, IN in_shopid int8, IN in_shopname varchar, IN in_categoryid int8, IN in_categoryname varchar, IN in_platformid int4, IN in_listeddate date, IN in_favorites int4, IN in_stock int8, IN in_discount int4, IN in_stockadd int8, IN in_stocknew int8, IN in_linksalesqty int8, IN in_yearsalesqty int8, IN in_linksalesamt float8, IN in_yearsalesamt float8) RETURNS "void"
	AS $BODY$BEGIN
    LOOP
        -- first try to update the key
        UPDATE women_clothing_item SET itemname= in_itemname, salesqty = in_salesqty, salesamt = in_salesamt, discountPrice = in_discountPrice, similarPrice = in_similarPrice, shopID = in_shopID, shopName = in_shopName, categoryID = in_categoryID, categoryName = in_categoryName, platformID = in_platformID, listedDate = in_listedDate, favorites = in_favorites, stock = in_stock, discount = in_discount, stockAdd = in_stockAdd, stockNew = in_stockNew, linkSalesqty = in_linkSalesqty, yearSalesqty = in_yearSalesqty, linkSalesamt = in_linkSalesamt, yearSalesamt = in_yearSalesamt WHERE itemid = in_itemid and dateRange = in_dateRange;
        IF found THEN
            RETURN;
        END IF;
        -- not there, so try to insert the key
        -- if someone else inserts the same key concurrently,
        -- we could get a unique-key failure
        BEGIN
            INSERT INTO women_clothing_item(itemID, itemname, dateRange, salesqty, salesamt, discountPrice, similarPrice, shopID, shopName, categoryID, categoryName, platformID, listedDate, favorites, stock, discount, stockAdd, stockNew, linkSalesqty, yearSalesqty, linkSalesamt, yearSalesamt) VALUES (in_itemID, in_itemname, in_dateRange, in_salesqty, in_salesamt, in_discountPrice, in_similarPrice, in_shopID, in_shopName, in_categoryID, in_categoryName, in_platformID, in_listedDate, in_favorites, in_stock, in_discount, in_stockAdd, in_stockNew, in_linkSalesqty, in_yearSalesqty, in_linkSalesamt, in_yearSalesamt);
            RETURN;

        EXCEPTION WHEN unique_violation THEN
            -- Do nothing, and loop to try the UPDATE again.
        END;
    END LOOP;
END;$BODY$
	LANGUAGE plpgsql
	COST 100
	CALLED ON NULL INPUT
	SECURITY INVOKER
	VOLATILE;




------ 4.1.6、生成 cube视图

drop view if EXISTS elengjing.women_clothing_item_view_v2;
CREATE VIEW IF NOT EXISTS elengjing.women_clothing_item_view_v2
AS
select
 t1.itemid
,t1.daterange
,cast(t3.years as string) as years
,t3.yearquartal
,t3.yearmonth
,t3.weekperiodmmdd
,t1.salesqty
,t1.salesamt
,t1.similarprice
,t1.shopid
,t1.discountPrice
,t1.shopname
,t1.categoryid
,t1.categoryname
,cast(t1.platformid as BIGINT) as platformid
,if(t1.discount > 10 or t1.discount = 0, 10, t1.discount) as discount
,if(t1.stockAdd >= 20, t1.stockAdd, 0) as stockAdd
,t1.stockNew
,if(t1.stockNew > 0, 1, 0) as isnew
,cast(t1.listedDate as date) as listedDate

,t2.tuan
,t2.mianliao
,t2.banxing
,t2.fengge
,t2.gongyi
,t2.kuanshi
,t2.qunxing
,t2.yaoxing
,t2.kuxing
,t2.xiuxing
,t2.baixing
,t2.lingzi
,t2.qunchang
,t2.liliao
,t2.caizhichengfen
,t2.tianchongwu
,t2.chongrongliang
,t2.hanrongliang

from
elengjing.women_clothing_item t1
left join
elengjing.WOMEN_CLOTHING_ITEM_ATTR t2
on t1.itemid = t2.itemid
left JOIN
elengjing.dimdate t3
on t1.daterange = t3.daterange
;

---------------------------------------- women_clothing_item_unique ----------------------------------------------------
------------ 4.2、 导出 women_clothing_item_unique

------ 4.2.1、生成 women_clothing_item_unique 导出表

drop table if exists transforms.export_women_clothing_item_unique;
CREATE table  if not exists transforms.export_women_clothing_item_unique(
 itemid                bigint
,itemname              string
,daterange             date
,itemurl               string
,itemsubtitle          string
,mainpicurl            string
,listprice             decimal(20,2)
,discountprice         bigint
,salesqty              bigint
,salesamt              decimal(20,2)
,favorites             bigint
,stock                 bigint
,categoryid            bigint
,categoryname          string
,shopid                bigint
,shopname              string
,shopurl               string
,listeddate            string
,platformid            int

,towWeekSalesqty       bigint
,monthSalesqty         bigint
,shopsalesqty          bigint
,linkSalesqty          bigint
,yearSalesqty          bigint


,towWeekSalesamt       decimal(20,2)
,monthSalesamt         decimal(20,2)
,linkSalesamt          decimal(20,2)
,yearSalesamt          decimal(20,2)

,dailyPrice            decimal(20, 2)
,eventPrice            decimal(20, 2)
,dailyPriceRatio       decimal(20, 2)
,eventPriceRatio       decimal(20, 2)


,chongrongliang        String
,hanrongliang          String
,tuan                  String
,tianchongwu           String
,gongyi                String
,baixing               String
,caizhichengfen        String
,kuanshi               String
,banxing               String
,yaoxing               String
,xiuxing               String
,qunxing               String
,qunchang              String
,kuxing                String
,liliao                String
,mianliao              String
,lingzi                String
,fengge                String
)
;


insert into table transforms.export_women_clothing_item_unique
select

 t1.itemid
,t1.itemname
,t1.daterange
,t1.itemurl
,t1.itemsubtitle
,t1.mainpicurl
,t1.listprice
,t1.discountprice
,t1.salesqty
,t1.salesamt
,t1.favorites
,t1.stock
,t1.categoryid
,t1.categoryname
,t1.shopid
,t1.shopname
,t1.shopurl
,t1.listeddate
,t1.platformid
,t1.towWeekSalesqty
,t1.monthSalesqty
,t1.shopsalesqty
,t1.linkSalesqty
,t1.yearSalesqty

,t1.towWeekSalesamt
,t1.monthSalesamt
,t1.linkSalesamt
,t1.yearSalesamt
,t1.dailyPrice
,t1.eventPrice
,t1.dailyPriceRatio
,t1.eventPriceRatio


,t2.chongrongliang
,t2.hanrongliang
,t2.tuan
,t2.tianchongwu
,t2.gongyi
,t2.baixing
,t2.caizhichengfen
,t2.kuanshi
,t2.banxing
,t2.yaoxing
,t2.xiuxing
,t2.qunxing
,t2.qunchang
,t2.kuxing
,t2.liliao
,t2.mianliao
,t2.lingzi
,t2.fengge

from
elengjing.women_clothing_item_unique t1
left JOIN
elengjing.women_clothing_item_attr t2
on t1.itemid = t2.itemid
;


------ 4.2.3、pg 的数据表

---- 1、 创建主表
DROP TABLE IF EXISTS "public"."women_clothing_item_unique_history";
DROP TABLE IF EXISTS "public"."women_clothing_item_unique_current";
DROP TABLE IF EXISTS "public"."women_clothing_item_unique";
CREATE TABLE "public"."women_clothing_item_unique" (
    "itemid" int8 NOT NULL,
    "itemname" varchar COLLATE "default",
    "daterange" date,
    "itemurl" varchar COLLATE "default",
    "itemsubtitle" varchar COLLATE "default",
    "mainpicurl" varchar COLLATE "default",
    "listprice" float8,
    "discountprice" int8,
    "salesqty" int8,
    "salesamt" float8,
    "favorites" int8,
    "stock" int8,
    "categoryid" int8,
    "categoryname" varchar COLLATE "default",
    "shopid" int8,
    "shopname" varchar COLLATE "default",
    "shopurl" varchar COLLATE "default",
    "listeddate" date NULL,
    "platformid" int4,
    "towweeksalesqty" int8,
    "monthsalesqty" int8,
    "shopsalesqty" int8,
    "linksalesqty" int8,
    "yearsalesqty" int8,
    "towweeksalesamt" float8,
    "monthsalesamt" float8,
    "linksalesamt" float8,
    "yearsalesamt" float8,
    "dailyprice" float8,
    "eventprice" float8,
    "dailypriceratio" float8,
    "eventpriceratio" float8,

    "chongrongliang" varchar COLLATE "default",
    "hanrongliang" varchar COLLATE "default",
    "tuan" varchar COLLATE "default",
    "tianchongwu" varchar COLLATE "default",
    "gongyi" varchar COLLATE "default",
    "baixing" varchar COLLATE "default",
    "caizhichengfen" varchar COLLATE "default",
    "kuanshi" varchar COLLATE "default",
    "banxing" varchar COLLATE "default",
    "yaoxing" varchar COLLATE "default",
    "xiuxing" varchar COLLATE "default",
    "qunxing" varchar COLLATE "default",
    "qunchang" varchar COLLATE "default",
    "kuxing" varchar COLLATE "default",
    "liliao" varchar COLLATE "default",
    "mianliao" varchar COLLATE "default",
    "lingzi" varchar COLLATE "default",
    "fengge" varchar COLLATE "default"
)
WITH (OIDS=FALSE);
ALTER TABLE "public"."women_clothing_item_unique" OWNER TO "elengjing";

-- ----------------------------
--  Primary key structure for table women_clothing_item_unique
-- ----------------------------
ALTER TABLE "public"."women_clothing_item_unique" ADD PRIMARY KEY ("itemid") NOT DEFERRABLE INITIALLY IMMEDIATE;




---- 2、 创建 字表
create table women_clothing_item_unique_history (check ( daterange < date '2017-08-08' ) ) INHERITS (women_clothing_item_unique);
create table women_clothing_item_unique_current (check ( daterange >= date '2017-08-08' ) ) INHERITS (women_clothing_item_unique);

ALTER TABLE "public"."women_clothing_item_unique_history" ADD PRIMARY KEY ("itemid") NOT DEFERRABLE INITIALLY IMMEDIATE;
ALTER TABLE "public"."women_clothing_item_unique_current" ADD PRIMARY KEY ("itemid") NOT DEFERRABLE INITIALLY IMMEDIATE;



---- 3、 创建 insert 函数

CREATE OR REPLACE FUNCTION women_clothing_item_unique_trigger() RETURNS TRIGGER AS $$
BEGIN
    DELETE from  women_clothing_item_unique where itemid = NEW.itemid;
    IF (
        NEW.daterange < date '2017-08-08'
    ) THEN
        INSERT INTO women_clothing_item_unique_history VALUES (NEW.*) ;
    ELSEIF (
        NEW.daterange >= date '2017-08-08'
    ) THEN
        INSERT INTO women_clothing_item_unique_current VALUES (NEW.*) ;
    ELSE
        RAISE EXCEPTION 'Date out of range!' ;
    END IF ;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;



---- 4、 创建 insert 触发器

CREATE TRIGGER insert_women_clothing_item_unique_partition_trigger
BEFORE INSERT ON women_clothing_item_unique
FOR EACH ROW EXECUTE PROCEDURE women_clothing_item_unique_trigger()
;


export
--connect
jdbc:postgresql://192.168.110.11:5432/elengjing
--username
elengjing
--password
Marcpoint2016
--table
women_clothing_item_unique_test
--num-mappers
85
--input-fields-terminated-by
'\001'
--input-null-string
"\\N"
--input-null-non-string
"\\N"
--export-dir
hdfs://nameservice1/user/hive/warehouse/transforms.db/export_women_clothing_item_unique/



nohup sqoop --options-file /home/script/tmp/sqoop.txt


CREATE INDEX daterange1  ON women_clothing_item_unique_history (daterange);
-- CREATE INDEX categoryid1 ON women_clothing_item_unique_201706 (categoryid);
CREATE INDEX shopid1 ON women_clothing_item_unique_history (shopid);


CREATE INDEX daterange2 ON women_clothing_item_unique_current (daterange);
-- CREATE INDEX categoryid2 ON women_clothing_item_unique_201707 (categoryid);
CREATE INDEX shopid2 ON women_clothing_item_unique_current (shopid);

CREATE INDEX discountprice2 ON women_clothing_item_unique_current (discountprice);




---------------------------------------- 上新 ----------------------------------------------------

------ 4.1.1、生成 women_clothing_item_new_and_supply 导出表

drop table if exists transforms.export_women_clothing_item_new_and_supply;
create table  if not exists transforms.export_women_clothing_item_new_and_supply(
 itemID                BIGINT
,itemname              string
,dateRange             date
,salesqty              BIGINT
,salesamt              decimal(20,2)
,discountPrice         decimal(20,2)
,similarPrice          BIGINT
,shopID                BIGINT
,shopName              string
,categoryID            BIGINT
,categoryName          string
,platformID            BIGINT
,listedDate            date
,favorites             BIGINT

,stock                 BIGINT
,discount              BIGINT

,stockAdd              BIGINT
,stockNew              BIGINT

,linkSalesqty          BIGINT
,yearSalesqty          BIGINT
,linkSalesamt          decimal(20,2)
,yearSalesamt          decimal(20,2)
,itemurl               string
,mainpicurl            string
)
;


insert into table transforms.export_women_clothing_item_new_and_supply
select
 w.itemid
,w.itemname
,w.daterange
,w.salesqty
,w.salesamt
,w.discountprice
,w.similarprice
,w.shopid
,w.shopname
,w.categoryid
,w.categoryname
,w.platformid
,u.listeddate
,w.favorites
,w.stock
,w.discount
,w.stockadd
,w.stocknew
,w.linksalesqty
,w.yearsalesqty
,w.linksalesamt
,w.yearsalesamt
,u.itemurl
,u.mainpicurl
from
elengjing.women_clothing_item w
left join
elengjing.women_clothing_item_unique u
on w.itemid = u.itemid
where
w.stocknew != 0
-- and w.daterange = '${hivevar:daterange}'
-- and w.daterange > '2017-08-20'
;

------ 4.1.2、导出的 sqoop

export
--connect
jdbc:postgresql://192.168.110.11:5432/elengjing
--username
elengjing
--password
Marcpoint2016
--call
save_women_clothing_item_new_and_supply
--num-mappers
50
--input-fields-terminated-by
'\001'
--input-null-string
"\\N"
--input-null-non-string
"\\N"
--export-dir
hdfs://nameservice1/user/hive/warehouse/transforms.db/export_women_clothing_item_new_and_supply/



sqoop --options-file /home/script/tmp/sqoop.txt




DROP TABLE IF EXISTS "public"."women_clothing_item_new_and_supply";
CREATE TABLE "public"."women_clothing_item_new_and_supply" (
    "itemid" int8 NOT NULL,
    "itemname" varchar COLLATE "default",
    "daterange" date,
    "salesqty" int8,
    "salesamt" float8,
    "discountprice" numeric,
    "similarprice" int8,
    "shopid" int8,
    "shopname" varchar COLLATE "default",
    "categoryid" int8,
    "categoryname" varchar COLLATE "default",
    "platformid" int4,
    "listeddate" date,
    "favorites" int4,
    "stock" int8,
    "discount" int4,
    "stockadd" int8,
    "stocknew" int8,
    "linksalesqty" int8,
    "yearsalesqty" int8,
    "linksalesamt" float8,
    "yearsalesamt" float8,
    "itemurl" varchar COLLATE "default",
    "mainpicurl" varchar COLLATE "default"
)
WITH (OIDS=FALSE);
ALTER TABLE "public"."women_clothing_item_new_and_supply" OWNER TO "elengjing";

-- ----------------------------
--  Primary key structure for table women_clothing_item_new_and_supply
-- ----------------------------
ALTER TABLE "public"."women_clothing_item_new_and_supply" ADD PRIMARY KEY ("itemid", "daterange") NOT DEFERRABLE INITIALLY IMMEDIATE;


------ 4.1.4、pg 表的 索引 及关联的视图
CREATE INDEX  "new_and_supply_daterange" ON "public"."women_clothing_item_new_and_supply" USING btree(daterange ASC NULLS LAST);
CREATE INDEX  "new_and_supply_item_id" ON "public"."women_clothing_item_new_and_supply" USING btree(itemid ASC NULLS LAST);
CREATE INDEX  "new_and_supply_shop_id" ON "public"."women_clothing_item_new_and_supply" USING btree(shopid ASC NULLS LAST);


CREATE OR REPLACE VIEW vwnewdetail AS
SELECT i.itemid,
    i.itemname,
    i.listeddate,
    i.itemurl,
    i.mainpicurl,
    i.stocknew,
    i.discountprice,
    i.categoryid,
    i.categoryname,
    u.monthsalesqty,
    i.daterange,
    i.shopid
FROM women_clothing_item_new_and_supply AS i
JOIN women_clothing_item_unique AS u
ON i.itemid = u.itemid;




CREATE OR REPLACE FUNCTION "public"."save_women_clothing_item_new_and_supply"(IN in_itemid int8, IN in_itemname varchar, IN in_daterange date, IN in_salesqty int8, IN in_salesamt float8, IN in_discountprice numeric, IN in_similarprice int8, IN in_shopid int8, IN in_shopname varchar, IN in_categoryid int8, IN in_categoryname varchar, IN in_platformid int4, IN in_listeddate date, IN in_favorites int4, IN in_stock int8, IN in_discount int4, IN in_stockadd int8, IN in_stocknew int8, IN in_linksalesqty int8, IN in_yearsalesqty int8, IN in_linksalesamt float8, IN in_yearsalesamt float8, IN in_itemurl varchar, IN in_mainpicurl varchar) RETURNS "void"
	AS $BODY$BEGIN
    LOOP
        -- first try to update the key
        UPDATE women_clothing_item_new_and_supply SET itemname= in_itemname, salesqty = in_salesqty, salesamt = in_salesamt, discountPrice = in_discountPrice, similarPrice = in_similarPrice, shopID = in_shopID, shopName = in_shopName, categoryID = in_categoryID, categoryName = in_categoryName, platformID = in_platformID, listedDate = in_listedDate, favorites = in_favorites, stock = in_stock, discount = in_discount, stockAdd = in_stockAdd, stockNew = in_stockNew, linkSalesqty = in_linkSalesqty, yearSalesqty = in_yearSalesqty, linkSalesamt = in_linkSalesamt, yearSalesamt = in_yearSalesamt, itemurl = in_itemurl, mainpicurl = in_mainpicurl WHERE itemid = in_itemid and dateRange = in_dateRange;
        IF found THEN
            RETURN;
        END IF;
        -- not there, so try to insert the key
        -- if someone else inserts the same key concurrently,
        -- we could get a unique-key failure
        BEGIN
            INSERT INTO women_clothing_item_new_and_supply(itemID, itemname, dateRange, salesqty, salesamt, discountPrice, similarPrice, shopID, shopName, categoryID, categoryName, platformID, listedDate, favorites, stock, discount, stockAdd, stockNew, linkSalesqty, yearSalesqty, linkSalesamt, yearSalesamt, itemurl, mainpicurl) VALUES (in_itemID, in_itemname, in_dateRange, in_salesqty, in_salesamt, in_discountPrice, in_similarPrice, in_shopID, in_shopName, in_categoryID, in_categoryName, in_platformID, in_listedDate, in_favorites, in_stock, in_discount, in_stockAdd, in_stockNew, in_linkSalesqty, in_yearSalesqty, in_linkSalesamt, in_yearSalesamt, in_itemurl, in_mainpicurl);
            RETURN;

        EXCEPTION WHEN unique_violation THEN
            -- Do nothing, and loop to try the UPDATE again.
        END;
    END LOOP;
END;$BODY$
	LANGUAGE plpgsql
	COST 100
	CALLED ON NULL INPUT
	SECURITY INVOKER
	VOLATILE;



---------------------------------------- 店铺 品类 max(discount) min( discount) ----------------------------------------------------

------ 5.1.1、生成 export_shop_category_discountprice 导出表

drop table if exists transforms.export_shop_category_discountprice;
create table  if not exists transforms.export_shop_category_discountprice(
 shopid                BIGINT
,categoryid            BIGINT
,min_discountprice     decimal(20,2)
,max_discountprice     decimal(20,2)
)
;


insert into table transforms.export_shop_category_discountprice
select
shopid,
categoryid,
min(discountprice) as min_discountprice,
max(discountprice) as max_discountprice
from
elengjing.women_clothing_item
group by shopid, categoryid
;

------ 4.1.2、导出的 sqoop

export
--connect
jdbc:postgresql://192.168.110.11:5432/elengjing
--username
elengjing
--password
Marcpoint2016
--call
save_shop_category_discountprice
--num-mappers
50
--input-fields-terminated-by
'\001'
--input-null-string
"\\N"
--input-null-non-string
"\\N"
--export-dir
hdfs://nameservice1/user/hive/warehouse/transforms.db/export_shop_category_discountprice/



sqoop --options-file /home/script/tmp/sqoop.txt




DROP TABLE IF EXISTS "public"."shop_category_discountprice";
CREATE TABLE "public"."shop_category_discountprice" (
    "shopid" int8 NOT NULL,
    "categoryid" int8,
    "min_discountprice" numeric,
    "max_discountprice" numeric

)
WITH (OIDS=FALSE);
ALTER TABLE "public"."shop_category_discountprice" OWNER TO "elengjing";

-- ----------------------------
--  Primary key structure for table shop_category_discount
-- ----------------------------
ALTER TABLE "public"."shop_category_discountprice" ADD PRIMARY KEY ("shopid", "categoryid") NOT DEFERRABLE INITIALLY IMMEDIATE;


------ 4.1.4、pg 表的 索引

CREATE OR REPLACE FUNCTION "public"."save_shop_category_discountprice"(IN in_shopid int8, IN in_categoryid int8, IN in_min_discountprice numeric, IN in_max_discountprice numeric) RETURNS "void"
	AS $BODY$BEGIN
    LOOP
        -- first try to update the key
        UPDATE shop_category_discountprice SET min_discountprice= in_min_discountprice, max_discountprice = in_max_discountprice WHERE shopid = in_shopid and categoryid = in_categoryid;
        IF found THEN
            RETURN;
        END IF;
        -- not there, so try to insert the key
        -- if someone else inserts the same key concurrently,
        -- we could get a unique-key failure
        BEGIN
            INSERT INTO shop_category_discountprice(shopid, categoryid, min_discountprice, max_discountprice) VALUES (in_shopid, in_categoryid, in_min_discountprice, in_max_discountprice);
            RETURN;

        EXCEPTION WHEN unique_violation THEN
            -- Do nothing, and loop to try the UPDATE again.
        END;
    END LOOP;
END;$BODY$
	LANGUAGE plpgsql
	COST 100
	CALLED ON NULL INPUT
	SECURITY INVOKER
	VOLATILE;



----------------------------------导入 discount_price_intervals  到 pg -------------------------------------


drop table if exists transforms.export_discount_price_intervals;
create table  if not exists transforms.export_discount_price_intervals(
 shop_id               BIGINT
,category_id           BIGINT
,intervals             string
)
;


insert into table transforms.export_discount_price_intervals
select
 w.shop_id
,w.category_id
,w.intervals
from
transforms.discount_price_intervals w
;



DROP TABLE IF EXISTS "public"."discount_price_intervals";
CREATE TABLE "public"."discount_price_intervals" (
	"shop_id" int8 NOT NULL,
	"category_id" int8 NOT NULL,
	"intervals" VARCHAR
)
WITH (OIDS=FALSE);
ALTER TABLE "public"."discount_price_intervals" OWNER TO "elengjing";




export
--connect
jdbc:postgresql://192.168.110.11:5432/elengjing
--username
elengjing
--password
Marcpoint2016
--table
discount_price_intervals
--num-mappers
85
--input-fields-terminated-by
'\001'
--input-null-string
"\\N"
--input-null-non-string
"\\N"
--export-dir
hdfs://nameservice1/user/hive/warehouse/extract.db/discountprice_intervals


nohup sqoop --options-file /home/script/tmp/sqoop.txt

alter table "discount_price_intervals" alter COLUMN intervals type json USING intervals::json ;





----------------------------------导入 shop  到 pg -------------------------------------


drop table if exists transforms.export_shop;
CREATE table  if not exists transforms.export_shop(
 id                bigint
,name              string
,brand             date
,platform          bigint
,url               string
,create_time       string
,update_time       string
,industry_id       string
,logo              string
,is_org            string
,has_tj            string
,has_tw            string
)
;


insert into table transforms.export_shop
select

 t1.shopid
,t1.shopname
,null
,t1.platform
,t1.shop_url
,null
,null
,16
,t1.logo_url
,null
,null
,null
from
elengjing.shop t1
;



export
--connect
jdbc:postgresql://192.168.110.11:5432/elengjing
--username
elengjing
--password
Marcpoint2016
--table
shop_new
--num-mappers
85
--input-fields-terminated-by
'\001'
--input-null-string
"\\N"
--input-null-non-string
"\\N"
--export-dir
hdfs://nameservice1/user/hive/warehouse/transforms.db/export_shop




nohup sqoop --options-file /home/script/tmp/sqoop.txt



-------------------------------------- 要删除的item -----------------------------------------
drop table if exists extract.itemid_del;
CREATE TABLE if not exists extract.itemid_del(
     daterange   date
    ,itemid bigint
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY'\001'
;

LOAD DATA LOCAL INPATH '/home/data/hy_item_del.csv' overwrite INTO TABLE extract.itemid_del
;

drop table if exists transforms.itemid_del;
CREATE TABLE if not exists transforms.itemid_del(
     daterange   date
    ,itemid bigint
)
CLUSTERED BY (itemid) INTO 480 BUCKETS
STORED AS ORC
;

insert into table transforms.itemid_del
select * from extract.itemid_del;

-------------------------------------- pg 备份 elengjing -----------------------------------------
#!/bin/sh

###################################################################################################
####                                   定时任务 主程序  pg 备份
####
####
####
####################################################################################################
## 公用环境变量及方法生效
. /etc/profile

DATERANGE=`date +%Y_%m_%d`
BAKNAME=/home/data/pg_backup/elengjing_${DATERANGE}.bz2
/usr/pgsql-9.5/bin/pg_dump -h 192.168.110.11 -U backup  -T attr_value_bak -T discount_price_intervals -T women_clothing_item_new_and_supply -T women_clothing_item -T women_clothing_item_unique -T women_clothing_item_unique_201706 -T women_clothing_item_unique_201707  -T women_clothing_item_unique_history -T women_clothing_item_unique_current -F t elengjing | bzip2 > ${BAKNAME}
scp ${BAKNAME} pg_backup@192.168.110.122:/home/pg_backup/elengjing_backup/
