--------------------------------------------------------------------------------------
----------                       初始化第一天的 自主计算数据
---------- 1、从增量表中提取shop 信息
----------
----------
----------
---------- 2、运行beeline
---------- beeline -u jdbc:hive2://mphd02:10000 -n hive -p hive1  --hivevar CSV_ITEM_FILE=hy_item_2016_12_23.csv  --hivevar BEGIN_DAY=2016-12-22  --hivevar DATE_RANGE=2016-12-22 -f  ./init_price.sql

----------
--------------------------------------------------------------------------------------

set mapred.fairscheduler.pool=poolHigh;
set ngmr.partition.automerge=true;
set ngmr.partition.mergesize.mb=200;

-- 1、数据入库

drop table if exists mpintranet.export_shop;
CREATE TABLE if not exists mpintranet.export_shop(
shopid BIGINT,
shopname STRING,
brand STRING,
platform STRING,
shop_url STRING,
is_registered int
)
;


insert into table mpintranet.export_shop

select
shopid,
max(shopname),
'',
max(PlatformId),
'',
''
FROM
mpintranet.women_clothing_item_new_item
where shopid is not null
group by shopid
;


