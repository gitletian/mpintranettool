-- 一、三个店铺：
   69302618,66098091,73401272
   select * from shop where shopid in (69302618,66098091,73401272)
-- 二、
-- 1、数据导入
drop table if exists mpintranet.nes;
CREATE TABLE  if not exists mpintranet.nes(
jiaoyichenggong STRING,
MonthlySalesQty BIGINT,
yuanjia STRING,
ItemID BIGINT,
ItemUrl STRING,
ItemName STRING,
DiscountPrice STRING,
ItemTotalSalesQty STRING,

MonthlySalesQty2 STRING,
TotalComments STRING,
ShopName STRING,
ShopId STRING,
crawlDate STRING,
MainPicUrl STRING,
f1 STRING,
f2 STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY'\t';


LOAD DATA LOCAL INPATH '/home/data/tmp/2.txt' OVERWRITE INTO TABLE mpintranet.nes;

-- 2、数据清洗
drop table if exists mpintranet.nes_new;
CREATE TABLE  if not exists mpintranet.nes_new(
jiaoyichenggong STRING,
MonthlySalesQty BIGINT,
yuanjia STRING,
ItemID BIGINT,
ItemUrl STRING,
ItemName STRING,
DiscountPrice STRING,
ItemTotalSalesQty STRING,

MonthlySalesQty2 STRING,
TotalComments STRING,
ShopName STRING,
ShopId STRING,
crawlDate STRING,
MainPicUrl STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY'\t';



insert into table mpintranet.nes_new
select
jiaoyichenggong,
MonthlySalesQty,
yuanjia,
ItemID,
ItemUrl,
concat(itemname," ",discountprice),
ItemTotalSalesQty,
MonthlySalesQty2,
TotalComments,
ShopName ,
ShopId ,
crawlDate,
date_sub(to_date(MainPicUrl),1),
f1
from
mpintranet.nes
where
MainPicUrl regexp "\\\\d{4}-\\\\d{2}-\\\\d{2}.*";



insert into mpintranet.nes_new
select
jiaoyichenggong,
MonthlySalesQty,
yuanjia,
ItemID,
ItemUrl,
itemname,
discountprice,
ItemTotalSalesQty,
MonthlySalesQty2,
TotalComments,
ShopName ,
ShopId ,
date_sub(to_date(crawldate),1),
MainPicUrl
from
mpintranet.nes
where
crawldate regexp "\\\\d{4}-\\\\d{2}-\\\\d{2}.*" ;


insert into mpintranet.nes_new
select
jiaoyichenggong,
MonthlySalesQty,
yuanjia,
ItemID,
ItemUrl,
concat(itemname," ",ItemTotalSalesQty),
MonthlySalesQty2,
TotalComments,
ShopName ,
ShopId ,
crawlDate,
MainPicUrl,
date_sub(to_date(f1),1),
f2
from
mpintranet.nes
where
f1 regexp "\\\\d{4}-\\\\d{2}-\\\\d{2}.*" ;





-- 3、抓取的数据进行去重
drop table if exists mpintranet.nes_new_qc;
CREATE TABLE  if not exists mpintranet.nes_new_qc(
jiaoyichenggong STRING,
MonthlySalesQty BIGINT,
yuanjia STRING,
ItemID BIGINT,
ItemUrl STRING,
ItemName STRING,
DiscountPrice STRING,
ItemTotalSalesQty STRING,

MonthlySalesQty2 STRING,
TotalComments STRING,
ShopName STRING,
ShopId STRING,
crawlDate STRING,
MainPicUrl STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY'\t';


insert into mpintranet.nes_new_qc
select
t.jiaoyichenggong,
t.MonthlySalesQty,
t.yuanjia,
t.ItemID,
t.ItemUrl,
t.itemname,
t.discountprice,
t.ItemTotalSalesQty,
t.MonthlySalesQty2,
t.TotalComments,
t.ShopName ,
t.ShopId ,
t.crawldate,
t.MainPicUrl
from
(
select
*
from
(
select tw1.* ,ROW_NUMBER() OVER (Partition By itemid,crawldate) AS rn
from mpintranet.nes_new tw1
) tw where tw.rn=1
) t;






-- 4、创建生意参谋的表

drop table if exists mpintranet.sycm_item;
CREATE TABLE  if not exists mpintranet.sycm_item(
suoshuzhongduan     STRING
,itemid   BIGINT
,itemtitle   STRING
,itemstatus   STRING
,itemlink   STRING
,views   STRING
,fangkeshu   STRING
,av_tingliutime   STRING
,tiaochulv   STRING
,xiadanzhuanhualv   STRING
,xiadanzhifuzhuanhualv  STRING
,zhifuzhuanhualv  STRING
,xiadanjiner  STRING
,xiadanshangpinjianshu  STRING
,xiadanmaijiashu  STRING
,paymonay  DECIMAL(20,2)
,payitemjianshu  BIGINT
,jiagouwucheshu  STRING
,av_fangkejiazhi  STRING
,dianjishu  STRING
,dianjizhuanhualv  STRING
,baoguangliang  STRING
,shoucangrenshu  STRING
,ssydzfmjs  STRING
,kedanjia  DECIMAL(20,2)
,sousuozhifzhl  STRING
,syydfks  STRING
,zfmjs  STRING
,szshcgtkje STRING
,szshcgtkbs STRING
,daterange STRING
,datatype INT
,shopid BIGINT
,shopname STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY'\t';



LOAD DATA LOCAL INPATH '/home/data/tmp/sycm/klder_csv/*.csv'  INTO TABLE mpintranet.sycm_item;
LOAD DATA LOCAL INPATH '/home/data/tmp/sycm/lily_csv/*.csv'  INTO TABLE mpintranet.sycm_item;
LOAD DATA LOCAL INPATH '/home/data/tmp/sycm/naersi_csv/*.csv'  INTO TABLE mpintranet.sycm_item;







-- 3、创建汇总表

drop table if exists mpintranet.sycm_qut;
CREATE TABLE  if not exists mpintranet.sycm_qut(
itemid   BIGINT
,amt   DECIMAL(20,2)
,qut   BIGINT
,daterange STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY'\t';

-- 4、插入汇总数据
-------------------2016-11-24 日汇总 -------------------
insert into mpintranet.sycm_qut
select
itemid,
sum(paymonay),
sum(payitemjianshu),
'2016-11-24'
from  mpintranet.sycm_item
where
daterange BETWEEN date_sub('2016-11-24', 29) and date_sub('2016-11-24', 1) and datatype = 1
group by itemid;

-------------------2016-11-25 日汇总 -------------------
insert into mpintranet.sycm_qut
select
itemid,
sum(paymonay),
sum(payitemjianshu),
'2016-11-25'
from  mpintranet.sycm_item
where
daterange BETWEEN date_sub('2016-11-25', 29) and date_sub('2016-11-25', 2)  and datatype = 1
group by itemid;

-------------------2016-11-26 日汇总 -------------------
insert into mpintranet.sycm_qut
select
itemid,
sum(paymonay),
sum(payitemjianshu),
'2016-11-26'
from  mpintranet.sycm_item
where
daterange BETWEEN date_sub('2016-11-26', 29) and date_sub('2016-11-26', 3)  and datatype = 1
group by itemid;

-------------------2016-11-27 日汇总 -------------------
insert into mpintranet.sycm_qut
select
itemid,
sum(paymonay),
sum(payitemjianshu),
'2016-11-27'
from  mpintranet.sycm_item
where
daterange BETWEEN date_sub('2016-11-27', 29) and date_sub('2016-11-27', 4)  and datatype = 1
group by itemid;



-- 5、计算每一天销量
--  5.1、创建结果表
 drop table if exists mpintranet.day_qut;
CREATE TABLE  if not exists mpintranet.day_qut(
itemid   BIGINT
,shopid BIGINT
,SalesQty   BIGINT
,daterange STRING
,DiscountPrice DECIMAL(20,2)
,SalesAmt DECIMAL(20,2)
,isnew INT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY'\t';

-- 5.2、进行单天统计

-------------------2016-11-24 日数据 -------------------
insert into mpintranet.day_qut
select
n.itemid,
n.shopid,
n.MonthlySalesQty - nvl(s.qut,0),
n.crawldate,
n.DiscountPrice,
n.DiscountPrice * (n.MonthlySalesQty - nvl(s.qut,0)),
if(s.itemid is null,0,1) as isnew
from
mpintranet.nes_new_qc n
left join
mpintranet.sycm_qut s
on n.crawldate = s.daterange and n.itemid = s.itemid
where n.crawldate = '2016-11-24';

-------------------2016-11-25 日数据 -------------------

insert into mpintranet.day_qut
select
n.itemid,
n.shopid,
n.MonthlySalesQty - nvl(s.qut,0) - nvl(t.qut,0),
n.crawldate,
n.DiscountPrice,
n.DiscountPrice * (n.MonthlySalesQty - nvl(s.qut,0) - nvl(t.qut,0)),
if(s.itemid is null and nvl(t.isnew,0) = 0 ,0,1)
from

mpintranet.nes_new_qc n
left join
mpintranet.sycm_qut s
on n.crawldate = s.daterange and n.itemid = s.itemid

left join
(
select
itemid,
sum(SalesQty) as qut,
if(sum(isnew)=0,0,1) as isnew
from
mpintranet.day_qut
where daterange BETWEEN '2016-11-24' and '2016-11-24'
group by itemid
) t
on n.itemid = t.itemid

where n.crawldate = '2016-11-25';

-------------------2016-11-26 日数据 -------------------
insert into mpintranet.day_qut
select
n.itemid,
n.shopid,
n.MonthlySalesQty - nvl(s.qut,0) - nvl(t.qut,0),
n.crawldate,
n.DiscountPrice,
n.DiscountPrice * (n.MonthlySalesQty - nvl(s.qut,0) - nvl(t.qut,0)),
if(s.itemid is null and nvl(t.isnew,0) = 0 ,0,1)
from

mpintranet.nes_new_qc n
left join
mpintranet.sycm_qut s
on n.crawldate = s.daterange and n.itemid = s.itemid

left join
(
select
itemid,
sum(SalesQty) as qut,
if(sum(isnew)=0,0,1) as isnew
from
mpintranet.day_qut
where daterange BETWEEN '2016-11-24' and '2016-11-25'
group by itemid
) t
on n.itemid = t.itemid

where n.crawldate = '2016-11-26';

-------------------2016-11-27 日数据 -------------------
insert into mpintranet.day_qut
select
n.itemid,
n.shopid,
n.MonthlySalesQty - nvl(s.qut,0) - nvl(t.qut,0),
n.crawldate,
n.DiscountPrice,
n.DiscountPrice * (n.MonthlySalesQty - nvl(s.qut,0) - nvl(t.qut,0)),
if(s.itemid is null and nvl(t.isnew,0) = 0 ,0,1)
from

mpintranet.nes_new_qc n
left join
mpintranet.sycm_qut s
on n.crawldate = s.daterange and n.itemid = s.itemid

left join
(
select
itemid,
sum(SalesQty) as qut,
if(sum(isnew)=0,0,1) as isnew
from
mpintranet.day_qut
where daterange BETWEEN '2016-11-24' and '2016-11-26'
group by itemid
) t
on n.itemid = t.itemid

where n.crawldate = '2016-11-27';



-- 6、输出数据


select
d.shopid,
d.itemid as "商品id",
d.daterange as "日期",
s.paymonay as "sycm-销售金额",
s.payitemjianshu as "sycm-销量",
s.kedanjia as "sycm-客单价",
d.SalesQty as "销量",
d.DiscountPrice as "单价",
d.DiscountPrice * d.SalesQty as "销售金额",
d.SalesAmt as "销售金额2",
d.isnew as "商品是否已存在"
from
mpintranet.day_qut d
left join
(select * from mpintranet.sycm_item where datatype = 1 ) s
on s.itemid = d.itemid and s.daterange = d.daterange
where
d.daterange in ("2016-11-24","2016-11-25","2016-11-26","2016-11-27")









