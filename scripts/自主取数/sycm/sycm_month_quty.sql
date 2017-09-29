--------------------------------------------------------------------------------------
----------                       计算 sycm 前 30 天 销量之和(只计算前30天的)
----------1、BEGIN_DATE
----------2、INDEX
----------   drop table if exists mpintranet.sycm_month_quty;
----------   CREATE TABLE  if not exists mpintranet.sycm_month_quty(
----------    itemid   BIGINT
----------   ,shopid    string
----------   ,shopname  string
----------   ,daterange date
----------
----------   ,paymonay  string
----------   ,szshcgtkje  string
----------   ,amt string
----------
----------   ,payitemjianshu  string
----------   ,szshcgtkbs  string
----------   ,qty string
----------
----------   )
----------   CLUSTERED BY (itemid) INTO 17 BUCKETS
----------   STORED AS ORC;
--------------------------------------------------------------------------------------
set ngmr.partition.automerge=true;
set ngmr.partition.mergesize.mb=200;
insert into mpintranet.sycm_month_quty
select
itemid,
max(shopid),
max(shopname),
'${hivevar:CURRENT_DAY}',

sum(paymonay),
sum(szshcgtkje),
sum(paymonay - szshcgtkje),

sum(payitemjianshu),
sum(szshcgtkbs),
sum(payitemjianshu - szshcgtkbs)

from  mpintranet.sycm_item
where
daterange BETWEEN date_sub('${hivevar:CURRENT_DAY}', 29) and '${hivevar:CURRENT_DAY}'
group by itemid
;





-----------结果统计表
------    select 
------    z.shopid,
------    z.shopname,
------    z.itemid,
------    z.daterange,
------    z.monthlysalesqty,
------    s.qty,
------
------    s.paymonay,
------    s.szshcgtkje,
------    s.amt,
------    s.payitemjianshu,
------    s.szshcgtkbs
------
------    from
------    mpintranet.women_clothing_item_new z
------    join
------    mpintranet.sycm_month_quty s
------    on z.itemid = s.itemid and z.daterange = s.daterange
------    where s.qty < z.monthlysalesqty
------    ;

