-- 1、修复 "2017-01-06","2017-01-07","2017-01-11","2017-01-13","2017-01-14","2017-01-21","2017-01-23","2017-01-24","2017-01-25","2017-01-27","2017-01-29","2017-01-30" 数据

drop table if exists mpintranet.women_clothing_item;
create table if not exists mpintranet.women_clothing_item like elengjing.women_clothing_item;


insert into mpintranet.women_clothing_item PARTITION(daterange)
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
,if(sku_day_salesqty < monthlysalesqty, sku_day_salesqty, salesqty)
,if(sku_day_salesqty < monthlysalesqty, sku_day_salesqty, salesqty) * discountprice
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
and if(sku_day_salesqty < monthlysalesqty, sku_day_salesqty, salesqty) > 0
and to_date(listeddate) != daterange
;


insert into mpintranet.women_clothing_item PARTITION(daterange)
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
where daterange in ("2017-01-06","2017-01-07","2017-01-11","2017-01-13","2017-01-14","2017-01-21","2017-01-23","2017-01-24","2017-01-25","2017-01-27","2017-01-29","2017-01-30")
and to_date(listeddate) = daterange
;




insert into mpintranet.women_clothing_item PARTITION(daterange)
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
and salesqty > 0
;


-------------------------------------------------- 2、修复 "2017-01-10","2017-01-11","2017-01-12","2017-01-13"



drop table if exists new_elengjing.women_clothing_item_repire;
create table new_elengjing.women_clothing_item_repire like new_elengjing.women_clothing_item;


drop table if exists mpintranet.repari_date;
create table mpintranet.repari_date
(
DateRange  DATE,
itemid BIGINT,
salesqty string
)
CLUSTERED BY (itemid) INTO 113 BUCKETS
STORED AS ORC;


with data_end as
(
    select
    itemid,
    monthlysalesqty
    from
    mpintranet.women_clothing_item
    where
    daterange = "2017-02-11"
    and sku_day_salesqty = 0
),
data_between as
(
    select
    itemid,
    sum(nvl(salesqty, 0)) as salesqty
    from
    mpintranet.women_clothing_item
    where daterange BETWEEN "2017-01-14" and "2017-02-10"
    group by itemid
)

insert into mpintranet.repari_date
select
"2017-01-13",
t1.itemid,
t1.monthlysalesqty - nvl(t2.salesqty, 0)
from
data_end t1
left join
data_between t2
on t1.itemid = t2.itemid
;


insert into new_elengjing.women_clothing_item_repire
select
 t1.itemid
,t1.itemurl
,t1.itemname
,t1.itemsubtitle
,t1.mainpicurl
,t1.itemattrdesc
,t1.listprice
,t1.discountprice
,t1.unitprice
,t1.unit
,if(t2.salesqty is not null and t2.salesqty < t1.monthlysalesqty and to_date(t1.listeddate) != t1.daterange, t2.salesqty, t1.salesqty)
,t1.salesamt
,t1.instock
,t1.skulist
,t1.favorites
,t1.totalcomments
,t1.brandname
,t1.categoryid
,t1.shopid
,t1.adchannel
,t1.platformid
,t1.keyword
,t1.monthlysalesqty
,t1.totalsalesqty
,t1.shopname
,t1.categoryname
,t1.listeddate
,t1.sku_day_salesqty
,t1.sku_day_salesamt
,t1.sku_day_stock_change
,t1.daterange
from
(
    select * from mpintranet.women_clothing_item where daterange = "2017-01-13"
) t1
left join
(
    select * from mpintranet.repari_date where daterange = "2017-01-13"
) t2
on
t1.itemid = t2.itemid
where if(t2.salesqty is not null and t2.salesqty < t1.monthlysalesqty and to_date(t1.listeddate) != t1.daterange, t2.salesqty, t1.salesqty) > 0
;




with data_end as
(
    select
    itemid,
    monthlysalesqty
    from
    mpintranet.women_clothing_item
    where
    daterange = "2017-02-10"
    and sku_day_salesqty = 0
),
data_between as
(
    select
    itemid,
    sum(nvl(salesqty, 0)) as salesqty
    from
    mpintranet.women_clothing_item
    where daterange BETWEEN "2017-01-14" and "2017-02-09"
    group by itemid
),
date_begin as
(
    select itemid,sum(salesqty) as salesqty from new_elengjing.women_clothing_item_repire where daterange BETWEEN "2017-01-13" and "2017-01-13" group by itemid
)


insert into mpintranet.repari_date
select
"2017-01-12",
t1.itemid,
t1.monthlysalesqty - nvl(t2.salesqty, 0) - nvl(t3.salesqty, 0)
from
data_end t1
left join
data_between t2
on t1.itemid = t2.itemid
left join
date_begin t3
on t1.itemid = t3.itemid
;



insert into new_elengjing.women_clothing_item_repire
select
 t1.itemid
,t1.itemurl
,t1.itemname
,t1.itemsubtitle
,t1.mainpicurl
,t1.itemattrdesc
,t1.listprice
,t1.discountprice
,t1.unitprice
,t1.unit
,if(t2.salesqty is not null and t2.salesqty < t1.monthlysalesqty and to_date(t1.listeddate) != t1.daterange, t2.salesqty, t1.salesqty)
,t1.salesamt
,t1.instock
,t1.skulist
,t1.favorites
,t1.totalcomments
,t1.brandname
,t1.categoryid
,t1.shopid
,t1.adchannel
,t1.platformid
,t1.keyword
,t1.monthlysalesqty
,t1.totalsalesqty
,t1.shopname
,t1.categoryname
,t1.listeddate
,t1.sku_day_salesqty
,t1.sku_day_salesamt
,t1.sku_day_stock_change
,t1.daterange
from
(
    select * from mpintranet.women_clothing_item where daterange = "2017-01-12"
) t1
left join
(
    select * from mpintranet.repari_date where daterange = "2017-01-12"
) t2
on
t1.itemid = t2.itemid
where if(t2.salesqty is not null and t2.salesqty < t1.monthlysalesqty and to_date(t1.listeddate) != t1.daterange, t2.salesqty, t1.salesqty) > 0
;





with data_end as
(
    select
    itemid,
    monthlysalesqty
    from
    mpintranet.women_clothing_item
    where
    daterange = "2017-02-09"
    and sku_day_salesqty = 0
),
data_between as
(
    select
    itemid,
    sum(nvl(salesqty, 0)) as salesqty
    from
    mpintranet.women_clothing_item
    where daterange BETWEEN "2017-01-14" and "2017-02-08"
    group by itemid
),
date_begin as
(
    select itemid,sum(salesqty) as salesqty from new_elengjing.women_clothing_item_repire where daterange BETWEEN "2017-01-12" and "2017-01-13" group by itemid
)


insert into mpintranet.repari_date
select
"2017-01-11",
t1.itemid,
t1.monthlysalesqty - nvl(t2.salesqty, 0) - nvl(t3.salesqty, 0)
from
data_end t1
left join
data_between t2
on t1.itemid = t2.itemid
left join
date_begin t3
on t1.itemid = t3.itemid
;



insert into new_elengjing.women_clothing_item_repire
select
 t1.itemid
,t1.itemurl
,t1.itemname
,t1.itemsubtitle
,t1.mainpicurl
,t1.itemattrdesc
,t1.listprice
,t1.discountprice
,t1.unitprice
,t1.unit
,if(t2.salesqty is not null and t2.salesqty < t1.monthlysalesqty and to_date(t1.listeddate) != t1.daterange, t2.salesqty, t1.salesqty)
,t1.salesamt
,t1.instock
,t1.skulist
,t1.favorites
,t1.totalcomments
,t1.brandname
,t1.categoryid
,t1.shopid
,t1.adchannel
,t1.platformid
,t1.keyword
,t1.monthlysalesqty
,t1.totalsalesqty
,t1.shopname
,t1.categoryname
,t1.listeddate
,t1.sku_day_salesqty
,t1.sku_day_salesamt
,t1.sku_day_stock_change
,t1.daterange
from
(
    select * from mpintranet.women_clothing_item where daterange = "2017-01-11"
) t1
left join
(
    select * from mpintranet.repari_date where daterange = "2017-01-11"
) t2
on
t1.itemid = t2.itemid
where if(t2.salesqty is not null and t2.salesqty < t1.monthlysalesqty and to_date(t1.listeddate) != t1.daterange, t2.salesqty, t1.salesqty) > 0
;





with data_end as
(
    select
    itemid,
    monthlysalesqty
    from
    mpintranet.women_clothing_item
    where
    daterange = "2017-02-08"
    and sku_day_salesqty = 0
),
data_between as
(
    select
    itemid,
    sum(nvl(salesqty, 0)) as salesqty
    from
    mpintranet.women_clothing_item
    where daterange BETWEEN "2017-01-14" and "2017-02-07"
    group by itemid
),
date_begin as
(
    select itemid,sum(salesqty) as salesqty from new_elengjing.women_clothing_item_repire where daterange BETWEEN "2017-01-11" and "2017-01-13" group by itemid
)


insert into mpintranet.repari_date
select
"2017-01-10",
t1.itemid,
t1.monthlysalesqty - nvl(t2.salesqty, 0) - nvl(t3.salesqty, 0)
from
data_end t1
left join
data_between t2
on t1.itemid = t2.itemid
left join
date_begin t3
on t1.itemid = t3.itemid
;



insert into new_elengjing.women_clothing_item_repire
select
 t1.itemid
,t1.itemurl
,t1.itemname
,t1.itemsubtitle
,t1.mainpicurl
,t1.itemattrdesc
,t1.listprice
,t1.discountprice
,t1.unitprice
,t1.unit
,if(t2.salesqty is not null and t2.salesqty < t1.monthlysalesqty and to_date(t1.listeddate) != t1.daterange, t2.salesqty, t1.salesqty)
,t1.salesamt
,t1.instock
,t1.skulist
,t1.favorites
,t1.totalcomments
,t1.brandname
,t1.categoryid
,t1.shopid
,t1.adchannel
,t1.platformid
,t1.keyword
,t1.monthlysalesqty
,t1.totalsalesqty
,t1.shopname
,t1.categoryname
,t1.listeddate
,t1.sku_day_salesqty
,t1.sku_day_salesamt
,t1.sku_day_stock_change
,t1.daterange
from
(
    select * from mpintranet.women_clothing_item where daterange = "2017-01-10"
) t1
left join
(
    select * from mpintranet.repari_date where daterange = "2017-01-10"
) t2
on
t1.itemid = t2.itemid
where if(t2.salesqty is not null and t2.salesqty < t1.monthlysalesqty and to_date(t1.listeddate) != t1.daterange, t2.salesqty, t1.salesqty) > 0
;


---------------------------------- 3、修复 "2017-01-01","2017-01-02","2017-01-03"

drop table if exists mpintranet.repari_date;
create table mpintranet.repari_date
(
DateRange  DATE,
itemid BIGINT,
salesqty string
)
CLUSTERED BY (itemid) INTO 113 BUCKETS
STORED AS ORC;


--- 修复 3 号 数据
with data_end as
(
    select
    nvl(t1.itemid, t2.itemid) as itemid,
    nvl(t1.monthlysalesqty, t2.monthlysalesqty) as monthlysalesqty
    from
    (
        select
        itemid,monthlysalesqty
        from
        mpintranet.women_clothing_item
        where
        daterange = "2017-02-01"
    ) t1
    full  join
    (
        select
        itemid,monthlysalesqty
        from
        mpintranet.women_clothing_item
        where
        daterange = "2017-01-31"
    ) t2
    on t1.itemid = t2.itemid
),
data_between as
(
    selectdui
    itemid,
    sum(if(daterange in ("2017-02-01","2017-01-31") and nvl(sku_day_salesqty, 0) < monthlysalesqty, nvl(sku_day_salesqty, 0), nvl(salesqty, 0) )) as salesqty
    from
    mpintranet.women_clothing_item
    where daterange BETWEEN "2017-01-04" and "2017-02-01"
    group by itemid
)

insert into mpintranet.repari_date
select
"2017-01-03",
t1.itemid,
t1.monthlysalesqty - nvl(t2.salesqty, 0)
from
data_end t1
left join
data_between t2
on t1.itemid = t2.itemid
where t1.monthlysalesqty - nvl(t2.salesqty, 0) > 0
;


--- 修复 2号 数据

with data_end as
(
    select
    itemid,
    monthlysalesqty
    from
    mpintranet.women_clothing_item
    where
    daterange = "2017-01-31"
),
data_between as
(
    select
    itemid,
    sum(if(daterange = "2017-01-31"  and nvl(sku_day_salesqty, 0) < monthlysalesqty, nvl(sku_day_salesqty, 0),nvl(salesqty, 0) )) as salesqty
    from
    mpintranet.women_clothing_item
    where daterange BETWEEN "2017-01-04" and "2017-01-31"
    group by itemid
),
date_begin as
(
    select itemid,sum(salesqty) as salesqty from mpintranet.repari_date where daterange BETWEEN "2017-01-03" and "2017-01-03" group by itemid
)

insert into mpintranet.repari_date
select
"2017-01-02",
t1.itemid,
t1.monthlysalesqty - nvl(t2.salesqty, 0) - nvl(t3.salesqty, 0)
from
data_end t1
left join
data_between t2
on t1.itemid = t2.itemid
left join
date_begin t3
on t1.itemid = t3.itemid
where t1.monthlysalesqty - nvl(t2.salesqty, 0) - nvl(t3.salesqty, 0) > 0
;




--- 修复 1 号 数据

with day_sum_3 as (
   select itemid, DateRange, salesqty from mpintranet.women_clothing_item where  daterange = "2017-01-03"
),
day_sum_1_2 as (
   select
   itemid,
   sum(salesqty) as salesqty
   from
   mpintranet.repari_date
   where
   daterange BETWEEN "2017-01-02" and "2017-01-03"
   group by itemid
)

insert into mpintranet.repari_date
select
"2017-01-01",
w1.itemid,
w1.salesqty - w2.salesqty
from
day_sum_3 w1
left join
day_sum_1_2 w2
on w1.itemid = w2.itemid
where w2.itemid is not null
and w1.salesqty - w2.salesqty > 0
;



---------------------------------- 修复 1 、2 、3 号 都无法根据月销量计算的数据



drop table if exists mpintranet.repari_date_1;
create table mpintranet.repari_date_1
(
DateRange  DATE,
itemid BIGINT,
salesqty string
)
CLUSTERED BY (itemid) INTO 113 BUCKETS
STORED AS ORC;


with day_sum_3 as (
   select itemid, DateRange, salesqty from mpintranet.women_clothing_item where  daterange = "2017-01-03"
),
day_sum_1_2 as (
   select
   itemid,
   sum(salesqty) as salesqty
   from
   mpintranet.repari_date
   where
   daterange BETWEEN "2017-01-02" and "2017-01-03"
   group by itemid
)

insert into mpintranet.repari_date_1
select
"2017-01-01",
w1.itemid,
floor(w1.salesqty * 0.46)
from
day_sum_3 w1
left join
day_sum_1_2 w2
on w1.itemid = w2.itemid
where w2.itemid is null
and floor(w1.salesqty * 0.46) > 0
;


with day_sum_3 as (
   select itemid, DateRange, salesqty from mpintranet.women_clothing_item where  daterange = "2017-01-03"
),
day_sum_1_2 as (
   select
   itemid,
   sum(salesqty) as salesqty
   from
   mpintranet.repari_date
   where
   daterange BETWEEN "2017-01-02" and "2017-01-03"
   group by itemid
)

insert into mpintranet.repari_date_1
select
"2017-01-02",
w1.itemid,
floor(w1.salesqty * 0.45)
from
day_sum_3 w1
left join
day_sum_1_2 w2
on w1.itemid = w2.itemid
where w2.itemid is null
and floor(w1.salesqty * 0.45) > 0
;



with day_sum_3 as (
   select itemid, DateRange, salesqty from mpintranet.women_clothing_item where  daterange = "2017-01-03"
),
day_sum_1_2 as (
   select
   itemid,
   sum(salesqty) as salesqty
   from
   mpintranet.repari_date
   where
   daterange BETWEEN "2017-01-02" and "2017-01-03"
   group by itemid
),
day1_sum_3 as
(
    select
    w1.itemid,
    w1.salesqty
    from
    day_sum_3 w1
    left join
    day_sum_1_2 w2
    on w1.itemid = w2.itemid
    where w2.itemid is null
),
day1_sum_2 as
(
    select
    itemid,
    sum(salesqty) salesqty
    from
    mpintranet.repari_date_1
    where
    daterange BETWEEN "2017-01-01" and "2017-01-02"
    group by itemid
)

insert into mpintranet.repari_date_1
select
"2017-01-03",
wb1.itemid,
wb1.salesqty - wb2.salesqty
from
day1_sum_3 wb1
left join
day1_sum_2 wb2
on wb1.itemid = wb2.itemid
where wb1.salesqty - wb2.salesqty > 0
;





----------------------------------------------------汇总修复后的数据------------------------------------------------------


drop table if exists mpintranet.women_clothing_item_new;
create table if not exists mpintranet.women_clothing_item_new like elengjing.women_clothing_item;


insert into mpintranet.women_clothing_item_new PARTITION(daterange)
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
mpintranet.women_clothing_item
where
daterange not in ("2017-01-01","2017-01-02","2017-01-03","2017-01-10","2017-01-11","2017-01-12","2017-01-13")
and salesqty > 0
;


-----2.1、插入 "2017-01-10","2017-01-11","2017-01-12","2017-01-13" 数据

insert into mpintranet.women_clothing_item_new PARTITION(daterange)
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
,salesqty * discountprice
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
new_elengjing.women_clothing_item_repire
where daterange in ("2017-01-10","2017-01-11","2017-01-12","2017-01-13")
and salesqty > 0
;


------ 2.2 插入 "2017-01-01","2017-01-02","2017-01-03" 号数据

insert into mpintranet.women_clothing_item_new PARTITION(daterange)
select
 t1.itemid
,t2.itemurl
,t2.itemname
,t2.itemsubtitle
,t2.mainpicurl
,t2.itemattrdesc
,t2.listprice
,t2.discountprice
,t2.unitprice
,t2.unit
,t1.salesqty
,t1.salesqty * t2.discountprice
,t2.instock
,t2.skulist
,t2.favorites
,t2.totalcomments
,t2.brandname
,t2.categoryid
,t2.shopid
,t2.adchannel
,t2.platformid
,t2.keyword
,t2.monthlysalesqty
,t2.totalsalesqty
,t2.shopname
,t2.categoryname
,t2.listeddate
,t2.sku_day_salesqty
,t2.sku_day_salesamt
,t2.sku_day_stock_change
,t1.daterange
from
mpintranet.repari_date t1
left join
(
    select * from mpintranet.women_clothing_item where daterange = '2017-01-03'
) t2
on t1.itemid = t2.itemid
where t1.salesqty > 0
;



insert into mpintranet.women_clothing_item_new PARTITION(daterange)
select
 t1.itemid
,t2.itemurl
,t2.itemname
,t2.itemsubtitle
,t2.mainpicurl
,t2.itemattrdesc
,t2.listprice
,t2.discountprice
,t2.unitprice
,t2.unit
,t1.salesqty
,t1.salesqty * t2.discountprice
,t2.instock
,t2.skulist
,t2.favorites
,t2.totalcomments
,t2.brandname
,t2.categoryid
,t2.shopid
,t2.adchannel
,t2.platformid
,t2.keyword
,t2.monthlysalesqty
,t2.totalsalesqty
,t2.shopname
,t2.categoryname
,t2.listeddate
,t2.sku_day_salesqty
,t2.sku_day_salesamt
,t2.sku_day_stock_change
,t1.daterange
from
mpintranet.repari_date_1 t1
left join
(
    select * from mpintranet.women_clothing_item where daterange = '2017-01-03'
) t2
on t1.itemid = t2.itemid
where t1.salesqty > 0
;
