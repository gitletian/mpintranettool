

-------------2017 之前数据处理-------
drop table if exists elengjing.women_clothing_item_unique_attrdesc;
create table elengjing.women_clothing_item_unique_attrdesc  like elengjing.women_clothing_item_unique;
insert into table elengjing.women_clothing_item_unique_attrdesc
select
  t.itemid
 ,t.daterange
 ,t.itemname
 ,t.itemurl
 ,t.listprice
 ,t.discountprice
 ,t.salesqty
 ,t.salesamt
 ,t.shopid
 ,t.adchannel
 ,t.brandname
 ,t.mainpicurl
 ,t.instock
 ,t.skulist
 ,t.categoryid
 ,t.itemattrdesc
 ,t.favorites
 ,t.totalcomments
 ,t.listeddate
 ,t.shopname
 ,t.platformid
 ,t.categoryname
from
(
select
t1.*,
ROW_NUMBER() OVER ( Partition By t1.itemid ORDER BY t1.daterange desc) AS rn
from
elengjing.women_clothing_item t1
where t1.daterange < '2017-01-01' and nvl(t1.itemattrdesc, '') != ''
) t
where t.rn = 1
;




drop table if exists elengjing.women_clothing_item_add_attrdesc;
create table elengjing.women_clothing_item_add_attrdesc like elengjing.women_clothing_item;

insert into table elengjing.women_clothing_item_add_attrdesc PARTITION(daterange)
select
t1.itemid
,t1.itemurl
,t1.itemname
,t1.itemsubtitle
,t1.mainpicurl
,if(nvl(t1.itemattrdesc, '') = '', t2.itemattrdesc, t1.itemattrdesc)
,t1.listprice
,t1.discountprice
,t1.unitprice
,t1.unit
,t1.salesqty
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
(select * from elengjing.women_clothing_item where daterange < '2017-01-01') t1
left join
elengjing.women_clothing_item_unique_attrdesc t2
on t1.itemid = t2.itemid
;



-------------2017 之后数据处理-------

drop table if exists elengjing.women_clothing_item_unique_2017;
create table elengjing.women_clothing_item_unique_2017  like elengjing.women_clothing_item_unique;
insert into table elengjing.women_clothing_item_unique_2017
select
  t.itemid
 ,t.daterange
 ,t.itemname
 ,t.itemurl
 ,t.listprice
 ,t.discountprice
 ,t.salesqty
 ,t.salesamt
 ,t.shopid
 ,t.adchannel
 ,t.brandname
 ,t.mainpicurl
 ,t.instock
 ,t.skulist
 ,t.categoryid
 ,t.itemattrdesc
 ,t.favorites
 ,t.totalcomments
 ,t.listeddate
 ,t.shopname
 ,t.platformid
 ,t.categoryname
from
(
select
t1.*,
ROW_NUMBER() OVER ( Partition By t1.itemid ORDER BY t1.daterange desc) AS rn
from
elengjing.women_clothing_item t1
where t1.daterange >='2017-01-01' and nvl(t1.itemattrdesc, '') != ''
) t
where t.rn = 1
;



insert into table elengjing.women_clothing_item_add_attrdesc PARTITION(daterange)
select
t1.itemid
,t1.itemurl
,t1.itemname
,t1.itemsubtitle
,t1.mainpicurl
,if(nvl(t1.itemattrdesc, '') = '', t2.itemattrdesc, t1.itemattrdesc)
,t1.listprice
,t1.discountprice
,t1.unitprice
,t1.unit
,t1.salesqty
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
(select * from elengjing.women_clothing_item where daterange >= '2017-01-01') t1
left join
elengjing.women_clothing_item_unique_2017 t2
on t1.itemid = t2.itemid
;




----------------------------------统计---------------------------------------------
483030460
select
count(1)
FROM
elengjing.women_clothing_item t1
where
t1.daterange < '2016-07-01'   and nvl(t1.itemattrdesc, '') = ''
;


345066933
select
count(1)
FROM
elengjing.women_clothing_item_add_attrdesc t1
where
t1.daterange < '2016-07-01'   and nvl(t1.itemattrdesc, '') = ''
;


---------------------
52922161

select
count(1)
FROM
elengjing.women_clothing_item t1
where
t1.daterange < '2017-01-01' and t1.daterange >= '2016-07-01'   and nvl(t1.itemattrdesc, '') = ''
;

14622012
select
count(1)
FROM
elengjing.women_clothing_item_add_attrdesc t1
where
t1.daterange < '2017-01-01' and t1.daterange >= '2016-07-01'   and nvl(t1.itemattrdesc, '') = ''
;


------------------
4361
select
count(1)
FROM
elengjing.women_clothing_item t1
where
t1.daterange >= '2017-01-01'   and nvl(t1.itemattrdesc, '') = ''
;

4169
select
count(1)
FROM
elengjing.women_clothing_item_add_attrdesc t1
where
t1.daterange >= '2017-01-01'   and nvl(t1.itemattrdesc, '') = ''
;


---------------------