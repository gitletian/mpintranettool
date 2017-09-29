--------------------------------------------------------------------------------------
----------                           全量 打标签 更新 脚本
----------1、全量数据打标签
----------2、更新属性表
----------
--------------------------------------------------------------------------------------
set mapred.fairscheduler.pool=poolHigh;
set ngmr.partition.automerge=true;
set ngmr.partition.mergesize.mb=200;
-------------------create women_clothing_item_attr ------------------------

drop table if exists t_elengjing.women_clothing_item_attr;
CREATE TABLE if not exists t_elengjing.women_clothing_item_attr(
itemid BIGINT,
attrname STRING,
attrvalue STRING,
errormessage STRING
)
CLUSTERED BY (ItemID) SORTED BY (ItemID) INTO 113 BUCKETS
STORED AS ORC;

----------------------------------------------------------------------------------------------------------------


insert into t_elengjing.women_clothing_item_attr
select
Tagged(tw.ItemID,tw.categoryid,tw.ItemName,tw.ItemAttrDesc)  as (itemid,attrname,attrvalue,errormessage)
from
(
select * ,ROW_NUMBER() OVER ( Partition By itemid order by DateRange desc) AS rn
from elengjing.women_clothing_item
) tw where tw.rn=1;

----------------------------------------------------women_clothing_item_attr_new--------------------------------------------------------------------------------------------------------------------
drop table if exists elengjing.women_clothing_item_attr_new;
CREATE TABLE if not exists elengjing.women_clothing_item_attr_new (
ItemID BIGINT,
AttrValue STRING
)
PARTITIONED BY(AttrName STRING)
CLUSTERED BY (AttrValue) SORTED BY (ItemID) INTO 31 BUCKETS
STORED AS ORC;

INSERT INTO table elengjing.women_clothing_item_attr_new PARTITION(AttrName)
SELECT ItemID, AttrValue, AttrName
from t_elengjing.women_clothing_item_attr
where ErrorMessage is null or ErrorMessage="";

drop table if exists elengjing.women_clothing_item_attr_bak;
use elengjing;
ALTER TABLE elengjing.women_clothing_item_attr RENAME TO elengjing.women_clothing_item_attr_bak;
ALTER TABLE elengjing.women_clothing_item_attr_new RENAME TO elengjing.women_clothing_item_attr;

----------------------------------------------------women_clothing_item_id--------------------------------------------------------------------------------------------------------------------
drop table if exists elengjing.women_clothing_item_id;
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

----------------------------------------------------women_clothing_item_attr_error--------------------------------------------------------------------------------------------------------------------
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
from t_elengjing.women_clothing_item_attr ta
where ta.ErrorMessage is not null and ta.ErrorMessage!="";