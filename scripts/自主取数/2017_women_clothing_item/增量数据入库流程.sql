--------------------------------------------------------------------------------------
----------                   elengjing 增量数据入库流程
----------
---------- beeline -u jdbc:hive2://mphd02:10000 -n elengjing --hivevar daterange=2017-08-06 --hivevar filedate=2017_08_06 -f /home/script/tmp/women_clothing_item_import.sql
----------
---------- sqoop --options-file /home/script/tmp/women_clothing_item_export.txt
---------- sqoop --options-file /home/script/tmp/women_clothing_item_unique_export.txt
--------------------------------------------------------------------------------------
----------
---------- set daterange=2017-08-02;
----------
---------- set filedate=2017_08_02;
--------------------------------------------------------------------------------------------- 一、201708 之后 增量 据入库--------------------------------------------------------------------------





-- ----------- 1.1、 合并后的数据入库
------ 1.1.1、数据入库
drop table if exists extract.women_clothing_item_load_add;
CREATE TABLE  if not exists extract.women_clothing_item_load_add(
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
FIELDS TERMINATED BY'\001';


LOAD DATA LOCAL INPATH '/home/data/combine_item_201708/hy_item_${hivevar:filedate}.bz2' OVERWRITE  INTO TABLE extract.women_clothing_item_load_add;


------ 1.1.2、去重过滤
drop table if exists extract.women_clothing_item_load_unique_add;
CREATE TABLE  if not exists extract.women_clothing_item_load_unique_add(
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


insert into extract.women_clothing_item_load_unique_add
select
 a.DateRange
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
from extract.women_clothing_item_load_add a
where
PlatformId in (7001, 7011)
and nvl(shopid, '') != ''
and nvl(CategoryId, 11111111111) in (1622,1623,1624,1629,1636,162103,162104,162105,162116,162201,162205,162401,162402,162403,162404,162701,162702,162703,50000671,50000697,50000852,50005065,50003509,50003510,50003511,50007068,50008897,50008898,50008899,50008900,50008901,50008903,50008904,50008905,50008906,50010850,50011277,50011404,50011411,50011412,50011413,50013194,50013196,50022566,50026651,121412004,121434004,123216004)
;


------ 1.1.3、数据汇总 到 基础表

drop table if exists transforms.women_clothing_item_new_dict_add;
CREATE TABLE  if not exists transforms.women_clothing_item_new_dict_add(
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


insert into transforms.women_clothing_item_new_dict_add
select
 t1.itemid
,t1.itemurl
,t1.itemname
,t1.ItemSubTitle
,t1.mainpicurl
,t1.itemattrdesc
,t1.listprice
,round(t1.salesAmt / t1.salesqty, 2)
,t1.UnitPrice
,t1.Unit
,t1.salesqty
,t1.salesamt
,t1.InStock
,t1.skulist
,t1.favorites
,t1.totalcomments
,t1.brandname
,t1.categoryid
,t1.shopid
,t1.adchannel
,t1.PlatformId
,t1.keyword
,t1.MonthlySalesQty
,t1.TotalSalesQty
,t4.shopname
,t4.shop_url
,t3.categoryname
,t1.listeddate
,t1.daterange
from
extract.women_clothing_item_load_unique_add t1
left join
elengjing.category t3
on t1.categoryid = t3.categoryid
left join
elengjing.shop t4
on t1.shopid = t4.shopid
;



insert into elengjing_base.women_clothing_item_new_dict PARTITION(month)
select *, substr(daterange, 1, 7) from transforms.women_clothing_item_new_dict_add
;


--------------------------------------------------------------------------------------------- 二、数据 加工处理--------------------------------------------------------------------------
------------ 2.1、 提取增量 elengjing.women_clothing_item

------ 2.1.1、计算 stockAdd、stockNew


drop table if exists transforms.item_stock_add;
create table  if not exists transforms.item_stock_add(
 itemID                BIGINT
,daterange             date
,stockAdd              BIGINT
,stockNew              BIGINT

)
stored as orc
;


with item_add as (
    select itemid, daterange, instock from transforms.women_clothing_item_new_dict_add  where nvl(instock, '') != ''
),
item_unique_desc as (
    select t.itemid, t.instock
    from
    (
        select tw1.itemid, tw1.stock as instock, ROW_NUMBER() OVER (Partition By tw1.itemid ORDER BY tw1.DateRange  desc) AS rn
        from elengjing.women_clothing_item tw1 where tw1.daterange < '${hivevar:daterange}' and nvl(tw1.stock, '') != ''
    ) t where t.rn=1
)

insert into table transforms.item_stock_add
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

------ 2.1.2、计算 linkSalesqty、 yearSalesqty、linkSalesamt、yearSalesamt


drop table if exists transforms.relative_kpi_add;
create table  if not exists transforms.relative_kpi_add(
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


insert into table transforms.relative_kpi_add
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
transforms.women_clothing_item_new_dict_add b
left join
(select itemid, daterange, salesqty, salesamt from elengjing.women_clothing_item where daterange = date_sub('${hivevar:daterange}', 1) ) l
on b.itemid = l.itemid
left join
(select itemid, daterange, salesqty, salesamt from elengjing.women_clothing_item where daterange = add_months('${hivevar:daterange}', -12)) y
on b.itemid = y.itemid
;


------ 2.1.3、合并 生成 women_clothing_item

drop table if exists transforms.women_clothing_item_add;
create table  if not exists transforms.women_clothing_item_add(
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
stored as orc
;


insert into table transforms.women_clothing_item_add
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
nvl(t3.yearSalesamt, 0)

from
transforms.women_clothing_item_new_dict_add t1
left join
transforms.item_stock_add t2
on t1.itemid = t2.itemid
left join
transforms.relative_kpi_add t3
on t1.itemid = t3.itemid
left JOIN
elengjing.women_clothing_item_unique t4
on t1.itemid = t4.itemid
;




insert into elengjing.women_clothing_item PARTITION(month)
select *, substr(daterange, 1, 7) from transforms.women_clothing_item_add
;





------ 2.1.4、 进行 export_women_clothing_item 增量 导出到 pg

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
transforms.women_clothing_item_add
;



------------ 2.2、 合并  elengjing.women_clothing_item_attr

------ 2.2.1、进行新的打标签

drop table if exists transforms.women_clothing_item_attr_add;
CREATE TABLE if not exists transforms.women_clothing_item_attr_add(
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


add jar /home/script/normal_servers/serverudf/elengjing/Tagged_test_9.jar;

drop temporary function if exists Tagged_extend;
create temporary function Tagged_extend as 'com.marcpoint.elengjing_extend.Tagged_extend_1';

set elengjing.var.separator_conf=new;
insert into transforms.women_clothing_item_attr_add
select
Tagged_extend(tw.ItemID,tw.categoryid,tw.shopid,tw.itemname,regexp_replace(tw.ItemAttrDesc, '\t', ' '))  as (itemid, shopid, categoryid, chongrongliang, hanrongliang, tuan, tuanwenhua, tianchongwu, gongyi, langxing, chengfenhanliang, baixing, caizhichengfen, kuanshi, banxing, lifubaixing, yaoxing, menyijin, xiuxing, xiuchang, qunxing, qunchang, kuxing, kuchang, jinxing, liliao, mianliao, lingzi, fengge, shiyongnianling, errormessage)
from
transforms.women_clothing_item_new_dict_add tw where nvl(tw.itemattrdesc, '') != ''
;


------ 2.2.2、合并新老标签


drop table if exists elengjing.women_clothing_item_attr_new;
CREATE TABLE if not exists elengjing.women_clothing_item_attr_new like elengjing.women_clothing_item_attr;

insert into table elengjing.women_clothing_item_attr_new
select
t1.*
FROM
elengjing.WOMEN_CLOTHING_ITEM_ATTR t1
left join
transforms.women_clothing_item_attr_add t2
on t1.itemid = t2.itemid
where t2.itemid is null
;


insert into table elengjing.women_clothing_item_attr_new
select
*
FROM
transforms.women_clothing_item_attr_add
;

use elengjing;
drop table if exists elengjing.women_clothing_item_attr_bak;
alter table elengjing.women_clothing_item_attr rename to elengjing.women_clothing_item_attr_bak;
alter table elengjing.women_clothing_item_attr_new rename to elengjing.women_clothing_item_attr;






------------ 2.3、 elengjing.women_clothing_item_unique 进行提取

------ 2.3.1、合并新老 unquie 表

drop table if exists elengjing.women_clothing_item_unique_new;
CREATE table  if not exists elengjing.women_clothing_item_unique_new like elengjing.women_clothing_item_unique;

insert into table elengjing.women_clothing_item_unique_new
select
t1.*
FROM
elengjing.women_clothing_item_unique t1
left join
transforms.women_clothing_item_add t2
on t1.itemid = t2.itemid
where t2.itemid is NULL
;


insert into table elengjing.women_clothing_item_unique_new
select
 t1.itemid
,t1.itemname
,t1.daterange
,t1.itemurl
,''
,t1.mainpicurl
,''
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

,null
,null
,null
,null
,null
,null
,null
,null
,null
,null
,null
,null
,null
FROM
transforms.women_clothing_item_add t1
;

------ 2.3.2、计算kpi 汇总

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


insert into table transforms.sum_salesqty
select
    t1.itemid,
    sum(if(datediff('${hivevar:daterange}', t1.daterange) between 0 and 13, nvl(t1.salesqty, 0), 0)) as tow_week_salesqty,
    sum(if(datediff('${hivevar:daterange}', t1.daterange) between 0 and 29, nvl(t1.salesqty, 0), 0)) as month_salesqty,

    sum(if(datediff(date_sub('${hivevar:daterange}', 30), t1.daterange) between 0 and 29, nvl(t1.salesqty, 0), 0)) as link_salesqty,
    sum(if(datediff(add_months('${hivevar:daterange}', -12), t1.daterange) between 0 and 29, nvl(t1.salesqty, 0), 0)) as year_salesqty,

    sum(if(datediff('${hivevar:daterange}', t1.daterange) between 0 and 13, nvl(t1.salesamt, 0), 0)) as tow_week_salesamt,
    sum(if(datediff('${hivevar:daterange}', t1.daterange) between 0 and 29, nvl(t1.salesamt, 0), 0)) as month_salesamt,

    sum(if(datediff(date_sub('${hivevar:daterange}', 30), t1.daterange) between 0 and 29, nvl(t1.salesamt, 0), 0)) as link_salesamt,
    sum(if(datediff(add_months('${hivevar:daterange}', -12), t1.daterange) between 0 and 29, nvl(t1.salesamt, 0), 0)) as year_salesamt

FROM
elengjing.women_clothing_item t1
group by t1.itemid
;



------ 2.3.3、重新合并 kpi 和 价格预测指标

use elengjing;
drop table if exists elengjing.women_clothing_item_unique_bak;
alter table elengjing.women_clothing_item_unique rename to elengjing.women_clothing_item_unique_bak;

CREATE table  if not exists elengjing.women_clothing_item_unique like elengjing.women_clothing_item_unique_bak;

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
elengjing.women_clothing_item_unique_new t1
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


drop table if exists elengjing.women_clothing_item_unique_new;



------ 2.3.4、导出 women_clothing_item_unique 到 pg


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
where t1.daterange = '${hivevar:daterange}'

;






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
and w.daterange = '${hivevar:daterange}'
;

