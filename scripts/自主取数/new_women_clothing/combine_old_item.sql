--------------------------------------------------------------------------------------
----------                       合并旧的 itme
----------1、
----------2、
----------
----------3、
--------------------------------------------------------------------------------------
set mapred.fairscheduler.pool=poolHigh;
set ngmr.partition.automerge=true;
set ngmr.partition.mergesize.mb=200;

-- 1、数据入库
/**/

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
where daterange = '${hivevar:DATE_RANGE}'
;

-- 2、 进行重新打标签
/**/




drop table if exists elengjing.women_clothing_item_attr_new;
CREATE TABLE if not exists elengjing.women_clothing_item_attr_new(
itemid BIGINT,
attrname STRING,
attrvalue STRING,
errormessage STRING
)
CLUSTERED BY (ItemID) SORTED BY (ItemID) INTO 113 BUCKETS
STORED AS ORC;

add jar /home/script/normal_servers/serverudf/elengjing/tagged_new_1.jar;


drop temporary function if exists Tagged_new_1;
create temporary function Tagged_new_1 as 'com.marcpoint.elengjing_new.Tagged_New_1';

insert into elengjing.women_clothing_item_attr_new
select
Tagged_new_1(tw.ItemID,tw.categoryid,tw.itemname,tw.ItemAttrDesc)  as (itemid,attrname,attrvalue,errormessage)
from
new_elengjing.women_clothing_item_unique_desc tw
;



drop table if exists elengjing.women_clothing_item_attr_bak;
CREATE TABLE if not exists elengjing.women_clothing_item_attr_bak (
ItemID BIGINT,
AttrValue STRING
)
PARTITIONED BY(AttrName STRING)
CLUSTERED BY (AttrValue) SORTED BY (ItemID) INTO 31 BUCKETS
STORED AS ORC;

INSERT INTO table elengjing.women_clothing_item_attr_bak PARTITION(AttrName)
SELECT ItemID, AttrValue, AttrName
from elengjing.women_clothing_item_attr_new
where ErrorMessage is null or ErrorMessage="";

INSERT INTO table elengjing.women_clothing_item_attr_bak PARTITION(AttrName)
SELECT ItemID, AttrValue, AttrName
from elengjing.women_clothing_item_attr_old
where ErrorMessage is null or ErrorMessage="";






drop table if exists elengjing.women_clothing_item_attr_error_bak;
CREATE TABLE if not exists elengjing.women_clothing_item_attr_error_bak (
ItemID BIGINT,
AttrName STRING,
AttrValue STRING,
ErrorMessage STRING
)
CLUSTERED BY (ItemID) SORTED BY (ItemID) INTO 113 BUCKETS
STORED AS ORC;

INSERT INTO table elengjing.women_clothing_item_attr_error_bak
SELECT ta.ItemID,ta.AttrName,ta.AttrValue,ta.ErrorMessage
from elengjing.women_clothing_item_attr_new ta
where ta.ErrorMessage is not null and ta.ErrorMessage!="";

INSERT INTO table elengjing.women_clothing_item_attr_error_bak
SELECT ta.ItemID,ta.AttrName,ta.AttrValue,ta.ErrorMessage
from elengjing.women_clothing_item_attr_old ta
where ta.ErrorMessage is not null and ta.ErrorMessage!="";



use elengjing;
drop table if exists elengjing.women_clothing_item_attr;
ALTER table elengjing.women_clothing_item_attr_bak RENAME to elengjing.women_clothing_item_attr;
drop table if exists elengjing.women_clothing_item_attr_error;
ALTER table elengjing.women_clothing_item_attr_error_bak RENAME to elengjing.women_clothing_item_attr_error;