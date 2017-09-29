
select  platform from elengjing.women_clothing_item  where  platform in ("淘宝", "天猫")    group by platform 

-- 1、 备份 老数据：


alter table women_clothing_item rename to women_clothing_item_20161231_bak;


----1,1 去重

drop table if exists elengjing.women_clothing_item_20161231;
CREATE TABLE  if not exists elengjing.women_clothing_item_20161231 like elengjing.women_clothing_item_20161231_bak;

insert into table elengjing.women_clothing_item_20161231 PARTITION(daterange)
select
  a.itemid
, a.itemname
, a.itemurl
, a.listprice
, a.discountprice
, a.salesqty
, a.salesamt
, a.shopid
, a.adchannel
, a.brandname
, a.mainpicurl
, a.instock
, a.skulist
, a.categoryid
, a.itemattrdesc
, a.favorites
, a.totalcomments
, a.listeddate
, a.shopname
, a.platform
, a.categoryname
, a.daterange
from
(
select t1.* ,ROW_NUMBER() OVER ( Partition By daterange, itemid ORDER BY listeddate desc) AS rn
from elengjing.women_clothing_item_20161231_bak t1
) a
where a.rn=1
;




-- 2、迁移 women_clothing_item 至新格式

drop table if exists elengjing.women_clothing_item;
CREATE TABLE  if not exists elengjing.women_clothing_item(
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



insert into elengjing.women_clothing_item PARTITION(daterange)
select
 itemid                 
,itemurl                
,itemname               
,""           
,mainpicurl             
,itemattrdesc           
,listprice              
,discountprice          
,""              
,""                   
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
,
case 
when platform = "淘宝" then 7001
when platform = "天猫" then 7011
else platform
end
,""                
,""        
,""          
,shopname               
,categoryname           
,listeddate
,""       
,""       
,""   
,daterange              

from
elengjing.women_clothing_item_20161231
;


-- 4、 重新创建附属表


drop table if exists elengjing.shop;
create table elengjing.shop like elengjing.shop_20161231;
insert into elengjing.shop select * from elengjing.shop_20161231;


-- 5、 生成总量表

drop table if exists elengjing.women_clothing_item_unique_tmp;
CREATE TABLE if not exists elengjing.women_clothing_item_unique_tmp(
ItemID BIGINT,
daterange date,
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
CLUSTERED BY (ItemID) SORTED BY (ItemID) INTO 211 BUCKETS
STORED AS ORC;



insert into table elengjing.women_clothing_item_unique_tmp
select
 itemid
,daterange
,itemname
,itemurl
,listprice
,discountprice
,salesqty
,salesamt
,shopid
,adchannel
,brandname
,mainpicurl
,instock
,skulist
,categoryid
,itemattrdesc
,favorites
,totalcomments
,listeddate
,shopname
,platform
,categoryname
from
(
select t1.* ,ROW_NUMBER() OVER ( Partition By itemid ORDER BY daterange desc) AS rn
from elengjing.women_clothing_item_20161231 t1
) a
where a.rn=1;





drop table if exists elengjing.women_clothing_item_unique;
CREATE TABLE if not exists elengjing.women_clothing_item_unique(
ItemID BIGINT,
daterange date,
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
CLUSTERED BY (ItemID) SORTED BY (ItemID) INTO 211 BUCKETS
STORED AS ORC;



insert into table elengjing.women_clothing_item_unique
select
 t1.itemid
,t1.daterange
,t1.itemname
,t1.itemurl
,t1.listprice
,t1.discountprice
,t1.salesqty
,t1.salesamt
,t1.shopid
,t1.adchannel
,t1.brandname
,t1.mainpicurl
,t1.instock
,t1.skulist
,t1.categoryid
,t1.itemattrdesc
,t1.favorites
,t1.totalcomments
,t1.listeddate
,t1.shopname
,t1.platform
,t1.categoryname
from
elengjing.women_clothing_item_unique_tmp t1
left JOIN
new_elengjing.women_clothing_item_unique_desc t2
on t1.itemid = t2.itemid
where t2.itemid is null
;


-- 6、重新打标签


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

add jar /home/script/normal_servers/serverudf/elengjing/Tagged_n_8.jar;

drop temporary function if exists Tagged_o_8;
create temporary function Tagged_o_8 as 'com.marcpoint.elengjing_new_2.Tagged_o_8';

insert into elengjing.women_clothing_item_attr_old
select
Tagged_o_8(tw.ItemID,tw.categoryid,tw.shopid,tw.itemname,tw.ItemAttrDesc)  as (itemid,shopid,categoryid,attrname,attrvalue,errormessage)
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

add jar /home/script/normal_servers/serverudf/elengjing/Tagged_n_8.jar;

drop temporary function if exists Tagged_n_8;
create temporary function Tagged_n_8 as 'com.marcpoint.elengjing_new_2.Tagged_n_8';

insert into elengjing.women_clothing_item_attr_new
select 
Tagged_n_8(tw.ItemID,tw.categoryid,tw.shopid,tw.itemname,tw.ItemAttrDesc)  as (itemid,shopid,categoryid,attrname,attrvalue,errormessage)
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



drop table if exists elengjing.women_clothing_item_attr_error;
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
from elengjing.women_clothing_item_attr_new ta 
where ta.ErrorMessage is not null and ta.ErrorMessage!="";

INSERT INTO table elengjing.women_clothing_item_attr_error 
SELECT ta.ItemID,ta.AttrName,ta.AttrValue,ta.ErrorMessage
from elengjing.women_clothing_item_attr_old ta 
where ta.ErrorMessage is not null and ta.ErrorMessage!="";




-- 9、汇总 item表


insert into elengjing.women_clothing_item PARTITION(daterange)
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
,sku_day_salesqty            
,sku_day_salesqty * discountprice           
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
,categoryname        
,listeddate
,sku_day_salesqty    
,sku_day_salesamt    
,sku_day_stock_change
,daterange           
from
new_elengjing.women_clothing_item
where daterange in ("2017-01-06","2017-01-07","2017-01-11","2017-01-13","2017-01-14","2017-01-21","2017-01-23","2017-01-24","2017-01-25","2017-01-27","2017-01-29","2017-01-30")
;


insert into elengjing.women_clothing_item PARTITION(daterange)
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
,categoryname        
,listeddate
,sku_day_salesqty    
,sku_day_salesamt    
,sku_day_stock_change
,daterange           
from
new_elengjing.women_clothing_item
where daterange not in ("2017-01-06","2017-01-07","2017-01-11","2017-01-13","2017-01-14","2017-01-21","2017-01-23","2017-01-24","2017-01-25","2017-01-27","2017-01-29","2017-01-30")
;



--------------------------------------
drop table if exists elengjing.women_clothing_item_new;
create table elengjing.women_clothing_item_new like elengjing.women_clothing_item;

insert into table elengjing.women_clothing_item_new PARTITION(daterange)
select * from elengjing.women_clothing_item where daterange < '2017-01-01';

insert into table elengjing.women_clothing_item_new PARTITION(daterange)
select * from new_elengjing.women_clothing_item;


alter table elengjing.women_clothing_item rename to elengjing.women_clothing_item_bak;
alter table elengjing.women_clothing_item_new rename to elengjing.women_clothing_item;



















