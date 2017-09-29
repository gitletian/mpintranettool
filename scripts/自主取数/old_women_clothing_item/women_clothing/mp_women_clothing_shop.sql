--------------------------------------------------------------------------------------
----------                        women_clothing_shop 入库脚本
----------1、shop的定时入库
----------
----------
--------------------------------------------------------------------------------------

-------------------create e_shop ------------------------

set mapred.fairscheduler.pool=poolHigh;
set ngmr.partition.automerge=true;
set ngmr.partition.mergesize.mb=200;
drop table if exists e_elengjing.shop;
/**/
CREATE EXTERNAL TABLE if not exists e_elengjing.shop(
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
ROW FORMAT DELIMITED
FIELDS TERMINATED BY'\001';


----------------------------load data-----------------------------------------------
LOAD DATA LOCAL INPATH '${hivevar:CSV_SHOP_FILE}' overwrite INTO TABLE e_elengjing.shop;

---- LOAD DATA LOCAL INPATH '/home/data/women_clothing_item/hy_shop/hy_shop_2016_11_11.csv' overwrite INTO TABLE e_elengjing.shop;


----------------------------full shop-----------------------------------------------


drop table if exists t_elengjing.shop;
---truncate table t_elengjing.shop;
CREATE TABLE if not exists t_elengjing.shop(
n_shopid BIGINT,
n_shopname STRING,
n_address STRING,
n_level INTEGER,
n_shop_url STRING,
n_favor BIGINT,
n_sellerid STRING,
n_nick STRING,
n_platform STRING,
n_dsr STRING,

s_shopid BIGINT,
s_shopname STRING,
s_address STRING,
s_level INTEGER,
s_shop_url STRING,
s_favor BIGINT,
s_sellerid STRING,
s_nick STRING,
s_platform STRING,
s_dsr STRING

)
STORED AS ORC;



insert into t_elengjing.shop
with new_shop as (
select
a.shopid,
a.shopname,
a.address,
a.level,
a.shop_url,
a.favor,
a.sellerid,
a.nick,
a.platform,
a.dsr
from
(
select s.*,ROW_NUMBER() OVER ( Partition By shopid)  rn
from e_elengjing.shop s where shopid is not null
) a
where a.rn=1
)
select
*
from
new_shop n
Full outer join
elengjing.shop s
on n.shopid = s.shopid;



------------------------------------END------------------------------------------------------------------































