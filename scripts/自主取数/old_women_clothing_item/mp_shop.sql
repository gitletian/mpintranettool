--------------------------------------------------------------------------------------
----------                        shop 同步 入库脚本
----------1、shop 同步到pg
----------2、增量shop 入库的 总shop表
----------
--------------------------------------------------------------------------------------
-------------------------------------init eventon----------------------------------------------------------
set mapred.fairscheduler.pool=poolHigh;
set ngmr.partition.automerge=true;
set ngmr.partition.mergesize.mb=200;
-----------------------------sqoop  to pg shop---------------------------------------------------

drop table if exists t_elengjing.export_shop;
CREATE TABLE if not exists t_elengjing.export_shop(
shopid BIGINT,
shopname STRING,
brand STRING,
platform STRING,
shop_url STRING,
is_registered int
);


insert into table t_elengjing.export_shop 
select
n_shopid,
n_shopname,
"",
n_platform,
n_shop_url,
""
from t_elengjing.shop where n_shopid is not null;



--------------------------------------------SHOP TO PG  BY UPDATE AND INSERT-------------------------------------------------------------------
-- CREATE FUNCTION merge_shop(in_id int4, in_name varchar(256), in_brand varchar(256), in_platform varchar(256), in_url varchar(256), in_is_registered bool ) RETURNS VOID AS
-- $$
-- BEGIN
--     LOOP
--         -- first try to update the key
--         UPDATE shop SET name = in_name, brand = in_brand, platform = in_platform, url = in_url, is_registered = in_is_registered WHERE id = in_id;
--         IF found THEN
--             RETURN;
--         END IF;
--         -- not there, so try to insert the key
--         -- if someone else inserts the same key concurrently,
--         -- we could get a unique-key failure
--         BEGIN
--             INSERT INTO shop(id, name, brand, platform, url, is_registered) VALUES (in_id, in_name, in_brand, in_platform, in_url, in_is_registered);
--             RETURN;
--         EXCEPTION WHEN unique_violation THEN
--             -- Do nothing, and loop to try the UPDATE again.
--         END;
--     END LOOP;
-- END;
-- $$
-- LANGUAGE plpgsql;

------------------------------add hive shop---------------------------------------------------------------------

drop table elengjing.shop_full;
CREATE TABLE if not exists elengjing.shop_full(
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
CLUSTERED BY (shopid) INTO 113 BUCKETS
STORED AS ORC;

-----------------error shop data ----------------------------
----- shopid in (62935024, 111700184, 114631985, 162400069, 102933181, 60391866, 106881032)
---insert into table elengjing.shop select 62935024,'性感玫瑰旗','上海',11,'https://shop62935024.taobao.com',2856,531832285,'sexyrose555','淘宝','4.6|4.8|4.7' from category limit 1;
---insert into table elengjing.shop select 111700184,'新奇魔术',null, 8,'https://shop111700184.taobao.com',372,2145193593,'nf678e','淘宝','4.8|4.9|4.9' from category limit 1;
---insert into table elengjing.shop select 114631985,'MZF铭之方旗',null, 9,'https://shop114631985.taobao.com',742,830584753,'zhangyanru1314','淘宝','4.8|4.8|4.8' from category limit 1;
---insert into table elengjing.shop select 162400069,'xiaoxi服饰旗',null, 6,'https://shop162400069.taobao.com',0,2912544791,'xiaoxi服饰','淘宝','4.9|4.9|4.9' from category limit 1;
---insert into table elengjing.shop select 102933181,'','广东广州', 9,'https://shop102933181.taobao.com',444,753355161,'','淘宝','4.7|4.8|4.7' from category limit 1;
---insert into table elengjing.shop select 60391866,'☆﹎汀汀貓潮流服饰','北京', 9,'https://shop60391866.taobao.com',2117,366716979,'尹湘缘88','淘宝','5|5|5' from category limit 1;
---insert into table elengjing.shop select 106881032,'初韵服饰','广东揭阳', 8,'https://shop106881032.taobao.com',344,1856377249,'大伙都惊呆了','淘宝','4.6|4.8|4.7' from category limit 1;



insert into table elengjing.shop_full
select
if(n_shopid is null,s_shopid,n_shopid),
if(n_shopid is null,s_shopname,n_shopname),
if(n_shopid is null,s_address,n_address),
if(n_shopid is null,s_level,n_level),
if(n_shopid is null,s_shop_url,n_shop_url),
if(n_shopid is null,s_favor,n_favor),
if(n_shopid is null,s_sellerid,n_sellerid),
if(n_shopid is null,s_nick,n_nick),
if(n_shopid is null,s_platform,n_platform),
if(n_shopid is null,s_dsr,n_dsr)
from t_elengjing.shop;

drop table if exists elengjing.shop_bak;
use elengjing;
ALTER TABLE elengjing.shop rename to elengjing.shop_bak;

ALTER TABLE elengjing.shop_full rename to elengjing.shop;



------------------------------------------------end---------------------------------------------------------------------























