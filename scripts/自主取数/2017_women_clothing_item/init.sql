--------------------------------------------------------------------------------------
----------                   初始化 elengjing 入库数据
----------
----------
--------------------------------------------------------------------------------------


--------------------------------------------------全量数据入库-------------------------------------------------------------

------------ 预备、旧 category 数据入库
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




------------ 一、旧 mjw shop 数据入库

------ 1.1、数据入库
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

------ 1.2、数据去重



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




------------ 二、旧 mjw item 数据入库

------ 2.1、数据入库

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



------ 2.2、数据去重

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

------ 2.3.1 转化skulist

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

------ 2.3、join 产生全量 新字典表

drop table if exists transforms.women_clothing_item_new_dict;
CREATE TABLE  if not exists transforms.women_clothing_item_new_dict(
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



insert into transforms.women_clothing_item_new_dict
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



--------------------------------------------------对数据进行提取处理-------------------------------------------------------------




------------ 三、提取 最新 itemattrdesc， 并进行打全量 标签。（目前只能全量）

drop table if exists transforms.women_clothing_item_description;
CREATE table  if not exists transforms.women_clothing_item_description(
 itemid                bigint
,itemname              string
,itemattrdesc          string
,categoryid            bigint
,shopid                bigint
,daterange             date
)
CLUSTERED BY (itemid) SORTED BY (shopid) INTO 117 BUCKETS
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
from transforms.women_clothing_item_new_dict tw1 where nvl(tw1.itemattrdesc, '') != ''
) t where t.rn=1
;



------ 3.1、新方式打标签

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
,errormessage STRING
)
CLUSTERED BY (itemid) SORTED BY (itemid) INTO 113 BUCKETS
STORED AS ORC
;



--------3.1.1 2017之前的数据 进行打标签
add jar /home/script/normal_servers/serverudf/elengjing/Tagged_1.jar;

drop temporary function if exists Tagged_e_o_18;

create temporary function Tagged_e_o_18 as 'com.marcpoint.elengjing_extend.Tagged_e_o_18';

insert into elengjing.women_clothing_item_attr
select
Tagged_e_o_18(tw.ItemID,tw.categoryid,tw.shopid,tw.itemname,regexp_replace(tw.ItemAttrDesc, '\t', ' '))  as (itemid, shopid, categoryid, chongrongliang, hanrongliang, tuan, tuanwenhua, tianchongwu, gongyi, langxing, chengfenhanliang, baixing, caizhichengfen, kuanshi, banxing, lifubaixing, yaoxing, menyijin, xiuxing, xiuchang, qunxing, qunchang, kuxing, kuchang, jinxing, liliao, mianliao, lingzi, fengge, errormessage)
from
transforms.women_clothing_item_description tw where tw.daterange < '2017-01-01'
;


--------3.1.2 2017之后的数据 进行打标签

add jar /home/script/normal_servers/serverudf/elengjing/Tagged_1.jar;
drop temporary function if exists Tagged_e_n_18;
create temporary function Tagged_e_n_18 as 'com.marcpoint.elengjing_extend.Tagged_e_n_18';

insert into elengjing.women_clothing_item_attr
select
Tagged_e_n_18(tw.ItemID,tw.categoryid,tw.shopid,tw.itemname,regexp_replace(tw.ItemAttrDesc, '\t', ' '))  as (itemid, shopid, categoryid, chongrongliang, hanrongliang, tuan, tuanwenhua, tianchongwu, gongyi, langxing, chengfenhanliang, baixing, caizhichengfen, kuanshi, banxing, lifubaixing, yaoxing, menyijin, xiuxing, xiuchang, qunxing, qunchang, kuxing, kuchang, jinxing, liliao, mianliao, lingzi, fengge, errormessage)
from
transforms.women_clothing_item_description tw where tw.daterange >= '2017-01-01'
;



---------------------------------------------------------------------------------------------对补充参数的修正--------------------------------------------------------------------------
------------ 四、 对补充参数的修正
---- ***1. 计算 stockAdd、stockNew (需计算每天的)


drop table if exists transforms.item_stock;
create table  if not exists transforms.item_stock(
 itemID                BIGINT
,daterange             date
,stockAdd              BIGINT
,stockNew              BIGINT

)
CLUSTERED BY (itemID)  INTO 511 BUCKETS
stored as orc
;


with item_add as (
    select itemid, instock, daterange from transforms.women_clothing_item_new_dict where daterange = '2016-07-01'
),
item_unique_desc as (
    select t.itemid, t.instock
    from
    (
        select tw1.* ,ROW_NUMBER() OVER (Partition By tw1.itemid ORDER BY tw1.DateRange  desc) AS rn
        from transforms.women_clothing_item_new_dict tw1 where tw1.daterange < '2016-07-01'
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


---- 2. 计算 linkSalesqty、 yearSalesqty、linkSalesamt、yearSalesamt (只需要初始化最近三十天的)


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
CLUSTERED BY (itemID)  INTO 511 BUCKETS
stored as orc
;


with base as (
select
itemid,
daterange,
salesqty,
salesamt
from
transforms.women_clothing_item_new_dict where daterange > '2016-12-01'
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
transforms.women_clothing_item_new_dict l
on b.itemid = l.itemid and b.daterange = date_sub(l.daterange, -1)
left join
transforms.women_clothing_item_new_dict y
on b.itemid = y.itemid and b.daterange = add_months(y.daterange, -12)
;

---------------------------------------------------------------------------------------------合并生成 women_clothing_item--------------------------------------------------------------------------
---- 1. 合并 生成 women_clothing_item

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
CLUSTERED BY (shopID)  INTO 511 BUCKETS
stored as orc
;



insert into table elengjing.women_clothing_item
select
t1.itemid,
t1.itemname,
t1.ItemUrl,
t1.MainPicUrl
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
t1.listedDate,
t1.favorites,

t1.instock,
if(t1.listprice is null, 0, nvl(floor(t1.discountprice * 1.0 / t1.listprice * 10), 0)),

nvl(t2.stockAdd, 0),
nvl(t2.stockNew, 0),

nvl(t3.linkSalesqty, 0),
nvl(t3.yearSalesqty, 0),
nvl(t3.linkSalesamt, 0),
nvl(t3.yearSalesamt, 0)

from
transforms.women_clothing_item_new_dict t1
left join
transforms.item_stock t2
on t1.itemid = t2.itemid and t1.daterange = t2.daterange
left join
transforms.relative_kpi t3
on t1.itemid = t3.itemid and t1.daterange = t3.daterange
;




---- 2. 去重 ，生成 women_clothing_item_unique_desc

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
CLUSTERED BY (shopID)  INTO 511 BUCKETS
stored as orc
;

insert into table elengjing.women_clothing_item_unique_desc
select
,t.itemid
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
,t.listeddate_min
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
    select tw1.* ,ROW_NUMBER() OVER (Partition By tw1.itemid ORDER BY tw1.DateRange  desc) as rn, min(daterange) OVER (Partition By tw1.itemid) as  listeddate_min
    from elengjing.women_clothing_item tw1
) t where t.rn=1
;

---------------------------------------------------------------------------------------------抽取 women_clothing_item_unique--------------------------------------------------------------------------


---- 1. 计算 计算kpi 汇总

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

set daterange = 2016-12-31;

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




---- 2. 合并生成 women_clothing_item_unique 表

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
CLUSTERED BY (itemid) SORTED BY (shopid) INTO 117 BUCKETS
stored as orc
;




insert into table elengjing.women_clothing_item_unique
select
 t1.itemid
,t1.itemname
,t1.daterange
,t1.itemurl
,t1.itemsubtitle
,t1.mainpicurl
,t1.listprice
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
on t1.itemid = t2.itemid
left join
price.predict_event e
on t1.itemid = e.itemid
left join
price.predict_daily d
on t1.itemid = d.itemid

;




--------------------------------------------------------------------------------------------- export 数据 to pg  --------------------------------------------------------------------------

---- 1. 导出最近三十天 的 women_clothing_item


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
;


insert into table transforms.export_women_clothing_item
select
,itemid
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
where daterange > '2016-11-30'
;



export
--connect
jdbc:postgresql://192.168.110.12:5432/elengjing
--username
elengjing
--password
Marcpoint2016
--call
save_women_clothing_item
--num-mappers
95
--input-fields-terminated-by
'\001'
--input-null-string
"\\N"
--input-null-non-string
"\\N"
--export-dir
hdfs://nameservice1/user/hive/warehouse/transforms.db/export_women_clothing_item/



sqoop --options-file ./itemsqoop.txt




---- 2. 导出最近三十天 的 women_clothing_item_unique


drop table if exists transforms.export_women_clothing_item_unique_attr;
CREATE table  if not exists transforms.export_women_clothing_item_unique_attr(
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


insert into table transforms.export_women_clothing_item_unique_attr
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
where t1.daterange > '2016-12-01'
;



export
--connect
jdbc:postgresql://192.168.110.12:5432/elengjing
--username
elengjing
--password
Marcpoint2016
--call
save_women_clothing_item_unique
--num-mappers
95
--input-fields-terminated-by
'\001'
--input-null-string
"\\N"
--input-null-non-string
"\\N"
--export-dir
hdfs://nameservice1/user/hive/warehouse/transforms.db/export_women_clothing_item_unique_desc/



sqoop --options-file ./itemsqoop.txt





--------------------------------------------------------------------------------------------- 生成cube 视图数据  --------------------------------------------------------------------------
drop view if EXISTS elengjing.women_clothing_item_v5;
CREATE VIEW IF NOT EXISTS elengjing.women_clothing_item_v5
AS
select
 t1.itemid
,t1.daterange
,cast(t3.year as string) as years
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
,t1.platformid
,t1.discount

,t1.stockAdd
,t1.stockNew

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