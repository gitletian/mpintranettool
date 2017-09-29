--------------------------------------------------------------------------------------
----------                   初始化 stockAdd  stockNew
----------
---------- sh test.sh '2016-07-01'
--------------------------------------------------------------------------------------

-----1、sh 命令

#!/bin/sh

. /etc/profile

DATERANGE=$1;
for n in {0..364};
do
    DATERANGE=`date -d "-${n} day ago $DATERANGE" +%Y-%m-%d`;
    echo $DATERANGE;
    beeline -u jdbc:hive2://mphd02:10000 -n elengjing --hivevar date_range=${DATERANGE} -f /home/script/tmp/init_stock/initstock.sql;

done


-----2、sql

drop table if exists transforms.women_clothing_item_instock;
CREATE table  if not exists transforms.women_clothing_item_instock(
 itemid                bigint
,instock               bigint
)
PARTITIONED BY (DateRange DATE)
stored as orc
;

set hive.exec.max.dynamic.partitions.pernode=1000;
set hive.exec.max.dynamic.partitions=1000;


insert into table transforms.women_clothing_item_instock PARTITION(daterange)
select itemid, instock, daterange from elengjing_base.women_clothing_item_new_dict where daterange < "2017-01-01"
;

insert into table transforms.women_clothing_item_instock PARTITION(daterange)
select itemid, stock, daterange from transforms.mjw_item_unique
;



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
    select itemid, daterange, instock from transforms.women_clothing_item_instock  where daterange = '${hivevar:date_range}'
),
item_unique_desc as (
    select t.itemid, t.instock
    from
    (
        select tw1.itemid, tw1.instock, ROW_NUMBER() OVER (Partition By tw1.itemid ORDER BY tw1.DateRange  desc) AS rn
        from transforms.women_clothing_item_instock tw1 where tw1.daterange < '${hivevar:date_range}'
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


