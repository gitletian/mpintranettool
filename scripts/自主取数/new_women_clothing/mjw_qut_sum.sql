--------------------------------------------------------------------------------------
----------                       计算 mjw 前 N 天 销量之和(只计算前30天的)
----------1、BEGIN_DATE
----------2、INDEX
---------- 				drop table if exists mpintranet.mjw_qut;
----------        CREATE TABLE  if not exists mpintranet.mjw_qut(
---------- 				itemid   BIGINT
---------- 				,amt   DECIMAL(20,2)
---------- 				,qut   BIGINT
----------        ,listeddate string
---------- 				)
---------- 				PARTITIONED BY (daterange date)
---------- 				CLUSTERED BY (itemid) INTO 17 BUCKETS
---------- 				STORED AS ORC;
--------------------------------------------------------------------------------------
set ngmr.partition.automerge=true;
set ngmr.partition.mergesize.mb=200;
insert into mpintranet.mjw_qut PARTITION (daterange='${hivevar:CURRENT_DAY}')
select
itemid,
sum(SalesAmt),
sum(SalesQty),
min(if(listeddate = '', null, listeddate))
from  elengjing.women_clothing_item
where
daterange BETWEEN date_sub('${hivevar:CURRENT_DAY}', 29) and date_sub('${hivevar:CURRENT_DAY}', ${hivevar:INDEX})
group by itemid;
