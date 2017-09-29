-- 1、数据导入
drop table if exists mpintranet.zzzq_taobao;
CREATE TABLE  if not exists mpintranet.zzzq_taobao(
skudisprice STRING,
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
f2 STRING,
f3 STRING,
f4 STRING,
f5 STRING,
f6 STRING,
f7 STRING,
f8 STRING,
f9 STRING,
f10 STRING,
f11 STRING,
f12 STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY'\t';


LOAD DATA LOCAL INPATH '/home/data/tmp/tao_price_2.txt'  INTO TABLE mpintranet.zzzq_taobao;

LOAD DATA LOCAL INPATH '/home/data/tmp/tmall_price.txt'  INTO TABLE mpintranet.zzzq_taobao;

LOAD DATA LOCAL INPATH '/home/data/tmp/tmall_tao_noskulist.txt'  INTO TABLE mpintranet.zzzq_taobao;

LOAD DATA LOCAL INPATH '/home/data/tmp/tmal_price_12_15.txt'  INTO TABLE mpintranet.zzzq_taobao;


-- 2、数据清洗
drop table if exists mpintranet.zzzq_taobao_new;
CREATE TABLE  if not exists mpintranet.zzzq_taobao_new(
skudisprice STRING,
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
daterange date,
MainPicUrl STRING,
f1 STRING,
f2 STRING,
f3 STRING,
f4 STRING,
f5 STRING,
f6 STRING,
f7 STRING,
f8 STRING,
f9 STRING,
f10 STRING,
f11 STRING,
f12 STRING
)
STORED AS ORC;


insert into table mpintranet.zzzq_taobao_new
select
skudisprice,
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

f1,

f2 ,
f3 ,
f4 ,
f5 ,
f6 ,
f7 ,
f8 ,
f9 ,
f10 ,
f11 ,
f12 ,
Null
from
mpintranet.zzzq_taobao
where
MainPicUrl regexp "\\\\d{4}-\\\\d{2}-\\\\d{2}.*" ;


insert into table mpintranet.zzzq_taobao_new
select
skudisprice,
jiaoyichenggong,
MonthlySalesQty,
yuanjia,
ItemID,
ItemUrl,
concat(itemname," ",discountprice, " ", ItemTotalSalesQty),
MonthlySalesQty2,
TotalComments,
ShopName ,
ShopId ,
crawlDate,
MainPicUrl,
date_sub(to_date(f1),1),
f2 ,
f3 ,
f4 ,
f5 ,
f6 ,
f7 ,
f8 ,
f9 ,
f10 ,
f11 ,
f12 ,
Null,
null
from
mpintranet.zzzq_taobao
where
f1 regexp "\\\\d{4}-\\\\d{2}-\\\\d{2}.*" ;



insert into table mpintranet.zzzq_taobao_new
select
skudisprice,
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
MainPicUrl,
f1,
f2 ,
f3 ,
f4 ,
f5 ,
f6 ,
f7 ,
f8 ,
f9 ,
f10 ,
f11 ,
f12
from
mpintranet.zzzq_taobao
where
crawldate regexp "\\\\d{4}-\\\\d{2}-\\\\d{2}.*" ;



三、二次清洗

drop table if exists mpintranet.zzzq_taobao_new_1;
CREATE TABLE  if not exists mpintranet.zzzq_taobao_new_1(
skudisprice STRING,
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
daterange date,
MainPicUrl STRING,
f1 STRING,
f2 STRING,
f3 STRING,
f4 STRING,
f5 STRING,
f6 STRING,
f7 STRING,
skuList STRING,
f9 STRING,
f10 STRING,
f11 STRING,
f12 STRING
)
STORED AS ORC;



insert into table mpintranet.zzzq_taobao_new_1
select
skudisprice,
jiaoyichenggong,
MonthlySalesQty,
yuanjia,
ItemID,
ItemUrl,
ItemName,
DiscountPrice,
ItemTotalSalesQty,

MonthlySalesQty2,
TotalComments,
ShopName ,
ShopId ,
daterange,
MainPicUrl,
f1,
f2 ,
f3 ,
f4 ,
f5 ,
f6 ,
f7 ,
f9 ,
f10 ,
f11 ,
f12 ,
null
from
mpintranet.zzzq_taobao_new
where
f9 regexp "sku" distribute by rand() ;


insert into table mpintranet.zzzq_taobao_new_1
select
skudisprice,
jiaoyichenggong,
MonthlySalesQty,
yuanjia,
ItemID,
ItemUrl,
ItemName,
DiscountPrice,
ItemTotalSalesQty,

MonthlySalesQty2,
TotalComments,
ShopName ,
ShopId ,
daterange,
MainPicUrl,
f1,
f2 ,
f3 ,
f4 ,
f5 ,
f6 ,
f7 ,
f8,
f9 ,
f10 ,
f11 ,
f12
from
mpintranet.zzzq_taobao_new
where
f9 is null  or f9 not regexp "sku"  distribute by rand() ;







-- 3、抓取的数据进行去重
drop table if exists mpintranet.zzzq_taobao_qc;
CREATE TABLE  if not exists mpintranet.zzzq_taobao_qc(
skudisprice STRING,
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
daterange date,
MainPicUrl STRING,
f1 STRING,
f2 STRING,
f3 STRING,
f4 STRING,
f5 STRING,
f6 STRING,
f7 STRING,
skuList STRING,
f9 STRING,
f10 STRING,
f11 STRING,
f12 STRING
)
STORED AS ORC;


insert into mpintranet.zzzq_taobao_qc
select
t.skudisprice,
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
t.daterange,
t.MainPicUrl,
t.f1  ,
t.f2  ,
t.f3  ,
t.f4  ,
t.f5  ,
t.f6  ,
t.f7  ,
t.skuList,
t.f9  ,
t.f10 ,
t.f11 ,
t.f12
from
(
select tw1.* ,ROW_NUMBER() OVER (Partition By itemid,daterange) AS rn
from mpintranet.zzzq_taobao_new_1 tw1
) t where t.rn=1;




四、数据进行  skuList 处理


drop table if exists mpintranet.zzzq_taobao_end;
CREATE TABLE  if not exists mpintranet.zzzq_taobao_end(
skudisprice_today STRING,
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
daterange date,
MainPicUrl STRING,
f1 STRING,
f2 STRING,
f3 STRING,
f4 STRING,
f5 STRING,
f6 STRING,
f7 STRING,
skuList STRING,
f9 STRING,
f10 STRING,
f11 STRING,
f12 STRING,
yesterday_skuList STRING,
skudisprice_yestady STRING
)
STORED AS ORC;


insert into mpintranet.zzzq_taobao_end
select
t.*,
y.skuList,
y.skudisprice
from
    mpintranet.zzzq_taobao_qc t
left join
    mpintranet.zzzq_taobao_qc y
on
    t.itemid = y.itemid and date_sub(t.daterange, 1) = y.daterange;





五、进行折扣价计算

drop table if exists mpintranet.zzzq_taobao_end_2;
CREATE TABLE  if not exists mpintranet.zzzq_taobao_end_2(
itemid BIGINT,
shopid BIGINT,
daterange date,
discountPrice STRING,
discountPrice2 STRING,
monthlySalesQty STRING,
skuList STRING,
yesterday_skuList STRING,
all_stock STRING,
all_qmt STRING,
platform STRING,
erro_info STRING,
skudisprice_today STRING,
skudisprice_yestady STRING
)
STORED AS ORC;


add file /home/script/test/udf_t57.py;

insert into mpintranet.zzzq_taobao_end_2
select

TRANSFORM (itemid, shopid, daterange, discountPrice, monthlySalesQty, jiaoyichenggong, skuList, yesterday_skuList, skudisprice_today, skudisprice_yestady)
USING "python udf_t57.py"
AS (itemid, shopid, daterange, discountPrice, discountPrice2, monthlySalesQty, skuList, yesterday_skuList, all_stock, all_qmt, platform, erro_info, skudisprice_today, skudisprice_yestady)
from mpintranet.zzzq_taobao_end
;





drop table if exists mpintranet.zzzq_taobao_end_3;
CREATE TABLE  if not exists mpintranet.zzzq_taobao_end_3(
itemid BIGINT,
shopid BIGINT,
daterange date,
discountPrice STRING,
discountPrice2 STRING,
monthlySalesQty STRING,
skuList STRING,
yesterday_skuList STRING,
all_stock STRING,
all_qmt STRING,
erro_info STRING,
platform STRING
)
STORED AS ORC;


insert into mpintranet.zzzq_taobao_end_3
select
itemid, shopid, daterange, discountPrice, discountPrice2, monthlySalesQty, skuList, yesterday_skuList, all_stock, all_qmt, erro_info, platform
from
mpintranet.zzzq_taobao_end_2
where daterange < cast("2016-12-09" as date)
;



insert into mpintranet.zzzq_taobao_end_3
select
itemid, shopid, daterange, discountPrice, discountPrice2, monthlySalesQty, skuList, yesterday_skuList, all_stock, all_qmt, erro_info, platform
from
mpintranet.zzzq_taobao_end_2
where daterange = cast("2016-12-09" as date)
;


insert into mpintranet.zzzq_taobao_end_3
select
t.itemid,
t.shopid,
t.daterange,
t.discountPrice,
if(t.all_qmt == 0, t3.discountPrice2, t.discountPrice2),
t.monthlySalesQty,
t.skuList,
t.yesterday_skuList,
t.all_stock,
t.all_qmt,
t.erro_info,
t.platform
from
mpintranet.zzzq_taobao_end_2  t
left join
mpintranet.zzzq_taobao_end_3 t3
on t.itemid = t3.itemid and date_sub(t.daterange, 1) = t3.daterange
where t.daterange = cast("2016-12-10" as date)
;


insert into mpintranet.zzzq_taobao_end_3
select
t.itemid,
t.shopid,
t.daterange,
t.discountPrice,
if(t.all_qmt == 0, t3.discountPrice2, t.discountPrice2),
t.monthlySalesQty,
t.skuList,
t.yesterday_skuList,
t.all_stock,
t.all_qmt,
t.erro_info,
t.platform
from
mpintranet.zzzq_taobao_end_2  t
left join
mpintranet.zzzq_taobao_end_3 t3
on t.itemid = t3.itemid and date_sub(t.daterange, 1) = t3.daterange
where t.daterange = cast("2016-12-11" as date)
;


insert into mpintranet.zzzq_taobao_end_3
select
t.itemid,
t.shopid,
t.daterange,
t.discountPrice,
if(t.all_qmt == 0, t3.discountPrice2, t.discountPrice2),
t.monthlySalesQty,
t.skuList,
t.yesterday_skuList,
t.all_stock,
t.all_qmt,
t.erro_info,
t.platform
from
mpintranet.zzzq_taobao_end_2  t
left join
mpintranet.zzzq_taobao_end_3 t3
on t.itemid = t3.itemid and date_sub(t.daterange, 1) = t3.daterange
where t.daterange = cast("2016-12-12" as date)
;

insert into mpintranet.zzzq_taobao_end_3
select
t.itemid,
t.shopid,
t.daterange,
t.discountPrice,
if(t.all_qmt == 0, t3.discountPrice2, t.discountPrice2),
t.monthlySalesQty,
t.skuList,
t.yesterday_skuList,
t.all_stock,
t.all_qmt,
t.erro_info,
t.platform
from
mpintranet.zzzq_taobao_end_2  t
left join
mpintranet.zzzq_taobao_end_3 t3
on t.itemid = t3.itemid and date_sub(t.daterange, 1) = t3.daterange
where t.daterange = cast("2016-12-13" as date)
;

insert into mpintranet.zzzq_taobao_end_3
select
t.itemid,
t.shopid,
t.daterange,
t.discountPrice,
if(t.all_qmt == 0, t3.discountPrice2, t.discountPrice2),
t.monthlySalesQty,
t.skuList,
t.yesterday_skuList,
t.all_stock,
t.all_qmt,
t.erro_info,
t.platform
from
mpintranet.zzzq_taobao_end_2  t
left join
mpintranet.zzzq_taobao_end_3 t3
on t.itemid = t3.itemid and date_sub(t.daterange, 1) = t3.daterange
where t.daterange = cast("2016-12-14" as date)
;

insert into mpintranet.zzzq_taobao_end_3
select
t.itemid,
t.shopid,
t.daterange,
t.discountPrice,
if(t.all_qmt == 0, t3.discountPrice2, t.discountPrice2),
t.monthlySalesQty,
t.skuList,
t.yesterday_skuList,
t.all_stock,
t.all_qmt,
t.erro_info,
t.platform
from
mpintranet.zzzq_taobao_end_2  t
left join
mpintranet.zzzq_taobao_end_3 t3
on t.itemid = t3.itemid and date_sub(t.daterange, 1) = t3.daterange
where t.daterange = cast("2016-12-15" as date)
;







-- 3、创建汇总表

drop table if exists mpintranet.mjw_qut;
CREATE TABLE  if not exists mpintranet.mjw_qut(
itemid   BIGINT
,amt   DECIMAL(20,2)
,qut   BIGINT
,daterange STRING
)
STORED AS ORC;



-- 4、插入汇总数据 －－－－29天汇总数据

-------------------2016-11-24 日汇总 -------------------
insert into mpintranet.mjw_qut
select
itemid,
sum(SalesAmt),
sum(SalesQty),
'2016-11-24'
from  elengjing.women_clothing_item
where
daterange BETWEEN date_sub('2016-11-24', 29) and date_sub('2016-11-24', 1)
group by itemid;

-------------------2016-11-25 日汇总 -------------------
insert into mpintranet.mjw_qut
select
itemid,
sum(SalesAmt),
sum(SalesQty),
'2016-11-25'
from  elengjing.women_clothing_item
where
daterange BETWEEN date_sub('2016-11-25', 29) and date_sub('2016-11-25', 2)
group by itemid;

-------------------2016-11-26 日汇总 -------------------
insert into mpintranet.mjw_qut
select
itemid,
sum(SalesAmt),
sum(SalesQty),
'2016-11-26'
from  elengjing.women_clothing_item
where
daterange BETWEEN date_sub('2016-11-26', 29) and date_sub('2016-11-26', 3)
group by itemid;

-------------------2016-11-27 日汇总 -------------------
insert into mpintranet.mjw_qut
select
itemid,
sum(SalesAmt),
sum(SalesQty),
'2016-11-27'
from  elengjing.women_clothing_item
where
daterange BETWEEN date_sub('2016-11-27', 29) and date_sub('2016-11-27', 4)
group by itemid;

-------------------2016-11-28 日汇总 -------------------
insert into mpintranet.mjw_qut
select
itemid,
sum(SalesAmt),
sum(SalesQty),
'2016-11-28'
from  elengjing.women_clothing_item
where
daterange BETWEEN date_sub('2016-11-28', 29) and date_sub('2016-11-28', 5)
group by itemid;

-------------------2016-11-29 日汇总 -------------------
insert into mpintranet.mjw_qut
select
itemid,
sum(SalesAmt),
sum(SalesQty),
'2016-11-29'
from  elengjing.women_clothing_item
where
daterange BETWEEN date_sub('2016-11-29', 29) and date_sub('2016-11-29', 6)
group by itemid;

-------------------2016-11-30 日汇总 -------------------
insert into mpintranet.mjw_qut
select
itemid,
sum(SalesAmt),
sum(SalesQty),
'2016-11-30'
from  elengjing.women_clothing_item
where
daterange BETWEEN date_sub('2016-11-30', 29) and date_sub('2016-11-30', 7)
group by itemid;

-------------------2016-12-01 日汇总 -------------------
insert into mpintranet.mjw_qut
select
itemid,
sum(SalesAmt),
sum(SalesQty),
'2016-12-01'
from  elengjing.women_clothing_item
where
daterange BETWEEN date_sub('2016-12-01', 29) and date_sub('2016-12-01', 8)
group by itemid;

-------------------2016-12-02 日汇总 -------------------
insert into mpintranet.mjw_qut
select
itemid,
sum(SalesAmt),
sum(SalesQty),
'2016-12-02'
from  elengjing.women_clothing_item
where
daterange BETWEEN date_sub('2016-12-02', 29) and date_sub('2016-12-02', 9)
group by itemid;

-------------------2016-12-03 日汇总 -------------------
insert into mpintranet.mjw_qut
select
itemid,
sum(SalesAmt),
sum(SalesQty),
'2016-12-03'
from  elengjing.women_clothing_item
where
daterange BETWEEN date_sub('2016-12-03', 29) and date_sub('2016-12-03', 10)
group by itemid;

-------------------2016-12-04 日汇总 -------------------
insert into mpintranet.mjw_qut
select
itemid,
sum(SalesAmt),
sum(SalesQty),
'2016-12-04'
from  elengjing.women_clothing_item
where
daterange BETWEEN date_sub('2016-12-04', 29) and date_sub('2016-12-04', 11)
group by itemid;

-------------------2016-12-05 日汇总 -------------------
insert into mpintranet.mjw_qut
select
itemid,
sum(SalesAmt),
sum(SalesQty),
'2016-12-05'
from  elengjing.women_clothing_item
where
daterange BETWEEN date_sub('2016-12-05', 29) and date_sub('2016-12-05', 12)
group by itemid;

-------------------2016-12-06 日汇总 -------------------
insert into mpintranet.mjw_qut
select
itemid,
sum(SalesAmt),
sum(SalesQty),
'2016-12-06'
from  elengjing.women_clothing_item
where
daterange BETWEEN date_sub('2016-12-06', 29) and date_sub('2016-12-06', 13)
group by itemid;

-------------------2016-12-07 日汇总 -------------------
insert into mpintranet.mjw_qut
select
itemid,
sum(SalesAmt),
sum(SalesQty),
'2016-12-07'
from  elengjing.women_clothing_item
where
daterange BETWEEN date_sub('2016-12-07', 29) and date_sub('2016-12-07', 14)
group by itemid;


-------------------2016-12-08 日汇总 -------------------
insert into mpintranet.mjw_qut
select
itemid,
sum(SalesAmt),
sum(SalesQty),
'2016-12-08'
from  elengjing.women_clothing_item
where
daterange BETWEEN date_sub('2016-12-08', 29) and date_sub('2016-12-08', 15)
group by itemid;


-------------------2016-12-09 日汇总 -------------------
insert into mpintranet.mjw_qut
select
itemid,
sum(SalesAmt),
sum(SalesQty),
'2016-12-09'
from  elengjing.women_clothing_item
where
daterange BETWEEN date_sub('2016-12-09', 29) and date_sub('2016-12-09', 16)
group by itemid;

-------------------2016-12-10 日汇总 -------------------
insert into mpintranet.mjw_qut
select
itemid,
sum(SalesAmt),
sum(SalesQty),
'2016-12-10'
from  elengjing.women_clothing_item
where
daterange BETWEEN date_sub('2016-12-10', 29) and date_sub('2016-12-10', 17)
group by itemid;

-------------------2016-12-11 日汇总 -------------------
insert into mpintranet.mjw_qut
select
itemid,
sum(SalesAmt),
sum(SalesQty),
'2016-12-11'
from  elengjing.women_clothing_item
where
daterange BETWEEN date_sub('2016-12-11', 29) and date_sub('2016-12-11', 18)
group by itemid;

-------------------2016-12-12 日汇总 -------------------
insert into mpintranet.mjw_qut
select
itemid,
sum(SalesAmt),
sum(SalesQty),
'2016-12-12'
from  elengjing.women_clothing_item
where
daterange BETWEEN date_sub('2016-12-12', 29) and date_sub('2016-12-12', 19)
group by itemid;

-------------------2016-12-13 日汇总 -------------------
insert into mpintranet.mjw_qut
select
itemid,
sum(SalesAmt),
sum(SalesQty),
'2016-12-13'
from  elengjing.women_clothing_item
where
daterange BETWEEN date_sub('2016-12-13', 29) and date_sub('2016-12-13', 20)
group by itemid;

-------------------2016-12-14 日汇总 -------------------
insert into mpintranet.mjw_qut
select
itemid,
sum(SalesAmt),
sum(SalesQty),
'2016-12-14'
from  elengjing.women_clothing_item
where
daterange BETWEEN date_sub('2016-12-14', 29) and date_sub('2016-12-14', 21)
group by itemid;

-------------------2016-12-15 日汇总 -------------------
insert into mpintranet.mjw_qut
select
itemid,
sum(SalesAmt),
sum(SalesQty),
'2016-12-15'
from  elengjing.women_clothing_item
where
daterange BETWEEN date_sub('2016-12-15', 29) and date_sub('2016-12-15', 22)
group by itemid;

-------------------2016-12-16 日汇总 -------------------
insert into mpintranet.mjw_qut
select
itemid,
sum(SalesAmt),
sum(SalesQty),
'2016-12-16'
from  elengjing.women_clothing_item
where
daterange BETWEEN date_sub('2016-12-16', 29) and date_sub('2016-12-16', 23)
group by itemid;

-------------------2016-12-17 日汇总 -------------------
insert into mpintranet.mjw_qut
select
itemid,
sum(SalesAmt),
sum(SalesQty),
'2016-12-17'
from  elengjing.women_clothing_item
where
daterange BETWEEN date_sub('2016-12-17', 29) and date_sub('2016-12-17', 24)
group by itemid;

-------------------2016-12-18 日汇总 -------------------
insert into mpintranet.mjw_qut
select
itemid,
sum(SalesAmt),
sum(SalesQty),
'2016-12-18'
from  elengjing.women_clothing_item
where
daterange BETWEEN date_sub('2016-12-18', 29) and date_sub('2016-12-18', 25)
group by itemid;

-------------------2016-12-19 日汇总 -------------------
insert into mpintranet.mjw_qut
select
itemid,
sum(SalesAmt),
sum(SalesQty),
'2016-12-19'
from  elengjing.women_clothing_item
where
daterange BETWEEN date_sub('2016-12-19', 29) and date_sub('2016-12-19', 26)
group by itemid;

-------------------2016-12-20 日汇总 -------------------
insert into mpintranet.mjw_qut
select
itemid,
sum(SalesAmt),
sum(SalesQty),
'2016-12-20'
from  elengjing.women_clothing_item
where
daterange BETWEEN date_sub('2016-12-20', 29) and date_sub('2016-12-20', 27)
group by itemid;

-------------------2016-12-21 日汇总 -------------------
insert into mpintranet.mjw_qut
select
itemid,
sum(SalesAmt),
sum(SalesQty),
'2016-12-21'
from  elengjing.women_clothing_item
where
daterange BETWEEN date_sub('2016-12-21', 29) and date_sub('2016-12-21', 28)
group by itemid;

-------------------2016-12-22 日汇总 -------------------
insert into mpintranet.mjw_qut
select
itemid,
sum(SalesAmt),
sum(SalesQty),
'2016-12-22'
from  elengjing.women_clothing_item
where
daterange BETWEEN date_sub('2016-12-22', 29) and date_sub('2016-12-22', 29)
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
,DiscountPrice2 DECIMAL(20,2)
,erro_info STRING
,platform STRING
,isnew INT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY'\t';

-- 5.2、进行单天统计

-------------------2016-11-24 日数据 -------------------
with day_sum as
(
select
itemid,
sum(SalesQty) as qut,
if(sum(isnew)=0,0,1) as isnew
from
mpintranet.day_qut
where daterange BETWEEN '2016-11-24' and '2016-11-23'
group by itemid
)

insert into mpintranet.day_qut
select
n.itemid,
n.shopid,
n.MonthlySalesQty - nvl(s.qut,0) - nvl(t.qut,0),
n.daterange,
n.DiscountPrice,
n.DiscountPrice2,
n.erro_info,
n.platform,
if(s.itemid is null and nvl(t.isnew ,0) = 0 ,0,1)
from
mpintranet.zzzq_taobao_end_3 n
left join
mpintranet.mjw_qut s
on n.daterange = s.daterange and n.itemid = s.itemid
left join
day_sum t
on n.itemid = t.itemid
where n.daterange = '2016-11-24';

-------------------2016-11-25 日数据 -------------------

with day_sum as
(
select
itemid,
sum(SalesQty) as qut,
if(sum(isnew)=0,0,1) as isnew
from
mpintranet.day_qut
where daterange BETWEEN '2016-11-24' and '2016-11-24'
group by itemid
)

insert into mpintranet.day_qut
select
n.itemid,
n.shopid,
n.MonthlySalesQty - nvl(s.qut,0) - nvl(t.qut,0),
n.daterange,
n.DiscountPrice,
n.DiscountPrice2,
n.erro_info,
n.platform,
if(s.itemid is null and nvl(t.isnew ,0) = 0 ,0,1)
from
mpintranet.zzzq_taobao_end_3 n
left join
mpintranet.mjw_qut s
on n.daterange = s.daterange and n.itemid = s.itemid
left join
day_sum t
on n.itemid = t.itemid
where n.daterange = '2016-11-25';

-------------------2016-11-26 日数据 -------------------

with day_sum as
(
select
itemid,
sum(SalesQty) as qut,
if(sum(isnew)=0,0,1) as isnew
from
mpintranet.day_qut
where daterange BETWEEN '2016-11-24' and '2016-11-25'
group by itemid
)

insert into mpintranet.day_qut
select
n.itemid,
n.shopid,
n.MonthlySalesQty - nvl(s.qut,0) - nvl(t.qut,0),
n.daterange,
n.DiscountPrice,
n.DiscountPrice2,
n.erro_info,
n.platform,
if(s.itemid is null and nvl(t.isnew ,0) = 0 ,0,1)
from
mpintranet.zzzq_taobao_end_3 n
left join
mpintranet.mjw_qut s
on n.daterange = s.daterange and n.itemid = s.itemid
left join
day_sum t
on n.itemid = t.itemid
where n.daterange = '2016-11-26';

-------------------2016-11-27 日数据 -------------------

with day_sum as
(
select
itemid,
sum(SalesQty) as qut,
if(sum(isnew)=0,0,1) as isnew
from
mpintranet.day_qut
where daterange BETWEEN '2016-11-24' and '2016-11-26'
group by itemid
)

insert into mpintranet.day_qut
select
n.itemid,
n.shopid,
n.MonthlySalesQty - nvl(s.qut,0) - nvl(t.qut,0),
n.daterange,
n.DiscountPrice,
n.DiscountPrice2,
n.erro_info,
n.platform,
if(s.itemid is null and nvl(t.isnew ,0) = 0 ,0,1)
from
mpintranet.zzzq_taobao_end_3 n
left join
mpintranet.mjw_qut s
on n.daterange = s.daterange and n.itemid = s.itemid
left join
day_sum t
on n.itemid = t.itemid
where n.daterange = '2016-11-27';

-------------------2016-11-28 日数据 -------------------

with day_sum as
(
select
itemid,
sum(SalesQty) as qut,
if(sum(isnew)=0,0,1) as isnew
from
mpintranet.day_qut
where daterange BETWEEN '2016-11-24' and '2016-11-27'
group by itemid
)

insert into mpintranet.day_qut
select
n.itemid,
n.shopid,
n.MonthlySalesQty - nvl(s.qut,0) - nvl(t.qut,0),
n.daterange,
n.DiscountPrice,
n.DiscountPrice2,
n.erro_info,
n.platform,
if(s.itemid is null and nvl(t.isnew ,0) = 0 ,0,1)
from
mpintranet.zzzq_taobao_end_3 n
left join
mpintranet.mjw_qut s
on n.daterange = s.daterange and n.itemid = s.itemid
left join
day_sum t
on n.itemid = t.itemid
where n.daterange = '2016-11-28';

-------------------2016-11-29 日数据 -------------------

with day_sum as
(
select
itemid,
sum(SalesQty) as qut,
if(sum(isnew)=0,0,1) as isnew
from
mpintranet.day_qut
where daterange BETWEEN '2016-11-24' and '2016-11-28'
group by itemid
)

insert into mpintranet.day_qut
select
n.itemid,
n.shopid,
n.MonthlySalesQty - nvl(s.qut,0) - nvl(t.qut,0),
n.daterange,
n.DiscountPrice,
n.DiscountPrice2,
n.erro_info,
n.platform,
if(s.itemid is null and nvl(t.isnew ,0) = 0 ,0,1)
from
mpintranet.zzzq_taobao_end_3 n
left join
mpintranet.mjw_qut s
on n.daterange = s.daterange and n.itemid = s.itemid
left join
day_sum t
on n.itemid = t.itemid
where n.daterange = '2016-11-29';

-------------------2016-11-30 日数据 -------------------

with day_sum as
(
select
itemid,
sum(SalesQty) as qut,
if(sum(isnew)=0,0,1) as isnew
from
mpintranet.day_qut
where daterange BETWEEN '2016-11-24' and '2016-11-29'
group by itemid
)

insert into mpintranet.day_qut
select
n.itemid,
n.shopid,
n.MonthlySalesQty - nvl(s.qut,0) - nvl(t.qut,0),
n.daterange,
n.DiscountPrice,
n.DiscountPrice2,
n.erro_info,
n.platform,
if(s.itemid is null and nvl(t.isnew ,0) = 0 ,0,1)
from
mpintranet.zzzq_taobao_end_3 n
left join
mpintranet.mjw_qut s
on n.daterange = s.daterange and n.itemid = s.itemid
left join
day_sum t
on n.itemid = t.itemid
where n.daterange = '2016-11-30';

-------------------2016-12-01 日数据 -------------------

with day_sum as
(
select
itemid,
sum(SalesQty) as qut,
if(sum(isnew)=0,0,1) as isnew
from
mpintranet.day_qut
where daterange BETWEEN '2016-11-24' and '2016-11-30'
group by itemid
)

insert into mpintranet.day_qut
select
n.itemid,
n.shopid,
n.MonthlySalesQty - nvl(s.qut,0) - nvl(t.qut,0),
n.daterange,
n.DiscountPrice,
n.DiscountPrice2,
n.erro_info,
n.platform,
if(s.itemid is null and nvl(t.isnew ,0) = 0 ,0,1)
from
mpintranet.zzzq_taobao_end_3 n
left join
mpintranet.mjw_qut s
on n.daterange = s.daterange and n.itemid = s.itemid
left join
day_sum t
on n.itemid = t.itemid
where n.daterange = '2016-12-01';

-------------------2016-12-02 日数据 -------------------

with day_sum as
(
select
itemid,
sum(SalesQty) as qut,
if(sum(isnew)=0,0,1) as isnew
from
mpintranet.day_qut
where daterange BETWEEN '2016-11-24' and '2016-12-01'
group by itemid
)

insert into mpintranet.day_qut
select
n.itemid,
n.shopid,
n.MonthlySalesQty - nvl(s.qut,0) - nvl(t.qut,0),
n.daterange,
n.DiscountPrice,
n.DiscountPrice2,
n.erro_info,
n.platform,
if(s.itemid is null and nvl(t.isnew ,0) = 0 ,0,1)
from
mpintranet.zzzq_taobao_end_3 n
left join
mpintranet.mjw_qut s
on n.daterange = s.daterange and n.itemid = s.itemid
left join
day_sum t
on n.itemid = t.itemid
where n.daterange = '2016-12-02';

-------------------2016-12-03 日数据 -------------------

with day_sum as
(
select
itemid,
sum(SalesQty) as qut,
if(sum(isnew)=0,0,1) as isnew
from
mpintranet.day_qut
where daterange BETWEEN '2016-11-24' and '2016-12-02'
group by itemid
)

insert into mpintranet.day_qut
select
n.itemid,
n.shopid,
n.MonthlySalesQty - nvl(s.qut,0) - nvl(t.qut,0),
n.daterange,
n.DiscountPrice,
n.DiscountPrice2,
n.erro_info,
n.platform,
if(s.itemid is null and nvl(t.isnew ,0) = 0 ,0,1)
from
mpintranet.zzzq_taobao_end_3 n
left join
mpintranet.mjw_qut s
on n.daterange = s.daterange and n.itemid = s.itemid
left join
day_sum t
on n.itemid = t.itemid
where n.daterange = '2016-12-03';

-------------------2016-12-04 日数据 -------------------

with day_sum as
(
select
itemid,
sum(SalesQty) as qut,
if(sum(isnew)=0,0,1) as isnew
from
mpintranet.day_qut
where daterange BETWEEN '2016-11-24' and '2016-12-03'
group by itemid
)

insert into mpintranet.day_qut
select
n.itemid,
n.shopid,
n.MonthlySalesQty - nvl(s.qut,0) - nvl(t.qut,0),
n.daterange,
n.DiscountPrice,
n.DiscountPrice2,
n.erro_info,
n.platform,
if(s.itemid is null and nvl(t.isnew ,0) = 0 ,0,1)
from
mpintranet.zzzq_taobao_end_3 n
left join
mpintranet.mjw_qut s
on n.daterange = s.daterange and n.itemid = s.itemid
left join
day_sum t
on n.itemid = t.itemid
where n.daterange = '2016-12-04';

-------------------2016-12-05 日数据 -------------------

with day_sum as
(
select
itemid,
sum(SalesQty) as qut,
if(sum(isnew)=0,0,1) as isnew
from
mpintranet.day_qut
where daterange BETWEEN '2016-11-24' and '2016-12-04'
group by itemid
)

insert into mpintranet.day_qut
select
n.itemid,
n.shopid,
n.MonthlySalesQty - nvl(s.qut,0) - nvl(t.qut,0),
n.daterange,
n.DiscountPrice,
n.DiscountPrice2,
n.erro_info,
n.platform,
if(s.itemid is null and nvl(t.isnew ,0) = 0 ,0,1)
from
mpintranet.zzzq_taobao_end_3 n
left join
mpintranet.mjw_qut s
on n.daterange = s.daterange and n.itemid = s.itemid
left join
day_sum t
on n.itemid = t.itemid
where n.daterange = '2016-12-05';


-------------------2016-12-06 日数据 -------------------

with day_sum as
(
select
itemid,
sum(SalesQty) as qut,
if(sum(isnew)=0,0,1) as isnew
from
mpintranet.day_qut
where daterange BETWEEN '2016-11-24' and '2016-12-05'
group by itemid
)

insert into mpintranet.day_qut
select
n.itemid,
n.shopid,
n.MonthlySalesQty - nvl(s.qut,0) - nvl(t.qut,0),
n.daterange,
n.DiscountPrice,
n.DiscountPrice2,
n.erro_info,
n.platform,
if(s.itemid is null and nvl(t.isnew ,0) = 0 ,0,1)
from
mpintranet.zzzq_taobao_end_3 n
left join
mpintranet.mjw_qut s
on n.daterange = s.daterange and n.itemid = s.itemid
left join
day_sum t
on n.itemid = t.itemid
where n.daterange = '2016-12-06';

-------------------2016-12-07 日数据 -------------------

with day_sum as
(
select
itemid,
sum(SalesQty) as qut,
if(sum(isnew)=0,0,1) as isnew
from
mpintranet.day_qut
where daterange BETWEEN '2016-11-24' and '2016-12-06'
group by itemid
)

insert into mpintranet.day_qut
select
n.itemid,
n.shopid,
n.MonthlySalesQty - nvl(s.qut,0) - nvl(t.qut,0),
n.daterange,
n.DiscountPrice,
n.DiscountPrice2,
n.erro_info,
n.platform,
if(s.itemid is null and nvl(t.isnew ,0) = 0 ,0,1)
from
mpintranet.zzzq_taobao_end_3 n
left join
mpintranet.mjw_qut s
on n.daterange = s.daterange and n.itemid = s.itemid
left join
day_sum t
on n.itemid = t.itemid
where n.daterange = '2016-12-07';


-------------------2016-12-08 日数据 -------------------

with day_sum as
(
select
itemid,
sum(SalesQty) as qut,
if(sum(isnew)=0,0,1) as isnew
from
mpintranet.day_qut
where daterange BETWEEN '2016-11-24' and '2016-12-07'
group by itemid
)

insert into mpintranet.day_qut
select
n.itemid,
n.shopid,
n.MonthlySalesQty - nvl(s.qut,0) - nvl(t.qut,0),
n.daterange,
n.DiscountPrice,
n.DiscountPrice2,
n.erro_info,
n.platform,
if(s.itemid is null and nvl(t.isnew ,0) = 0 ,0,1)
from
mpintranet.zzzq_taobao_end_3 n
left join
mpintranet.mjw_qut s
on n.daterange = s.daterange and n.itemid = s.itemid
left join
day_sum t
on n.itemid = t.itemid
where n.daterange = '2016-12-08';


-------------------2016-12-09 日数据 -------------------

with day_sum as
(
select
itemid,
sum(SalesQty) as qut,
if(sum(isnew)=0,0,1) as isnew
from
mpintranet.day_qut
where daterange BETWEEN '2016-11-24' and '2016-12-08'
group by itemid
)

insert into mpintranet.day_qut
select
n.itemid,
n.shopid,
n.MonthlySalesQty - nvl(s.qut,0) - nvl(t.qut,0),
n.daterange,
n.DiscountPrice,
n.DiscountPrice2,
n.erro_info,
n.platform,
if(s.itemid is null and nvl(t.isnew ,0) = 0 ,0,1)
from
mpintranet.zzzq_taobao_end_3 n
left join
mpintranet.mjw_qut s
on n.daterange = s.daterange and n.itemid = s.itemid
left join
day_sum t
on n.itemid = t.itemid
where n.daterange = '2016-12-09';

-------------------2016-12-10 日数据 -------------------

with day_sum as
(
select
itemid,
sum(SalesQty) as qut,
if(sum(isnew)=0,0,1) as isnew
from
mpintranet.day_qut
where daterange BETWEEN '2016-11-24' and '2016-12-09'
group by itemid
)

insert into mpintranet.day_qut
select
n.itemid,
n.shopid,
n.MonthlySalesQty - nvl(s.qut,0) - nvl(t.qut,0),
n.daterange,
n.DiscountPrice,
n.DiscountPrice2,
n.erro_info,
n.platform,
if(s.itemid is null and nvl(t.isnew ,0) = 0 ,0,1)
from
mpintranet.zzzq_taobao_end_3 n
left join
mpintranet.mjw_qut s
on n.daterange = s.daterange and n.itemid = s.itemid
left join
day_sum t
on n.itemid = t.itemid
where n.daterange = '2016-12-10';

-------------------2016-12-11 日数据 -------------------

with day_sum as
(
select
itemid,
sum(SalesQty) as qut,
if(sum(isnew)=0,0,1) as isnew
from
mpintranet.day_qut
where daterange BETWEEN '2016-11-24' and '2016-12-10'
group by itemid
)

insert into mpintranet.day_qut
select
n.itemid,
n.shopid,
n.MonthlySalesQty - nvl(s.qut,0) - nvl(t.qut,0),
n.daterange,
n.DiscountPrice,
n.DiscountPrice2,
n.erro_info,
n.platform,
if(s.itemid is null and nvl(t.isnew ,0) = 0 ,0,1)
from
mpintranet.zzzq_taobao_end_3 n
left join
mpintranet.mjw_qut s
on n.daterange = s.daterange and n.itemid = s.itemid
left join
day_sum t
on n.itemid = t.itemid
where n.daterange = '2016-12-11';


-------------------2016-12-12 日数据 -------------------

with day_sum as
(
select
itemid,
sum(SalesQty) as qut,
if(sum(isnew)=0,0,1) as isnew
from
mpintranet.day_qut
where daterange BETWEEN '2016-11-24' and '2016-12-11'
group by itemid
)

insert into mpintranet.day_qut
select
n.itemid,
n.shopid,
n.MonthlySalesQty - nvl(s.qut,0) - nvl(t.qut,0),
n.daterange,
n.DiscountPrice,
n.DiscountPrice2,
n.erro_info,
n.platform,
if(s.itemid is null and nvl(t.isnew ,0) = 0 ,0,1)
from
mpintranet.zzzq_taobao_end_3 n
left join
mpintranet.mjw_qut s
on n.daterange = s.daterange and n.itemid = s.itemid
left join
day_sum t
on n.itemid = t.itemid
where n.daterange = '2016-12-12';


-------------------2016-12-13 日数据 -------------------

with day_sum as
(
select
itemid,
sum(SalesQty) as qut,
if(sum(isnew)=0,0,1) as isnew
from
mpintranet.day_qut
where daterange BETWEEN '2016-11-24' and '2016-12-12'
group by itemid
)

insert into mpintranet.day_qut
select
n.itemid,
n.shopid,
n.MonthlySalesQty - nvl(s.qut,0) - nvl(t.qut,0),
n.daterange,
n.DiscountPrice,
n.DiscountPrice2,
n.erro_info,
n.platform,
if(s.itemid is null and nvl(t.isnew ,0) = 0 ,0,1)
from
mpintranet.zzzq_taobao_end_3 n
left join
mpintranet.mjw_qut s
on n.daterange = s.daterange and n.itemid = s.itemid
left join
day_sum t
on n.itemid = t.itemid
where n.daterange = '2016-12-13';


-------------------2016-12-14 日数据 -------------------

with day_sum as
(
select
itemid,
sum(SalesQty) as qut,
if(sum(isnew)=0,0,1) as isnew
from
mpintranet.day_qut
where daterange BETWEEN '2016-11-24' and '2016-12-13'
group by itemid
)

insert into mpintranet.day_qut
select
n.itemid,
n.shopid,
n.MonthlySalesQty - nvl(s.qut,0) - nvl(t.qut,0),
n.daterange,
n.DiscountPrice,
n.DiscountPrice2,
n.erro_info,
n.platform,
if(s.itemid is null and nvl(t.isnew ,0) = 0 ,0,1)
from
mpintranet.zzzq_taobao_end_3 n
left join
mpintranet.mjw_qut s
on n.daterange = s.daterange and n.itemid = s.itemid
left join
day_sum t
on n.itemid = t.itemid
where n.daterange = '2016-12-14';

-------------------2016-12-15 日数据 -------------------

with day_sum as
(
select
itemid,
sum(SalesQty) as qut,
if(sum(isnew)=0,0,1) as isnew
from
mpintranet.day_qut
where daterange BETWEEN '2016-11-24' and '2016-12-14'
group by itemid
)

insert into mpintranet.day_qut
select
n.itemid,
n.shopid,
n.MonthlySalesQty - nvl(s.qut,0) - nvl(t.qut,0),
n.daterange,
n.DiscountPrice,
n.DiscountPrice2,
n.erro_info,
n.platform,
if(s.itemid is null and nvl(t.isnew ,0) = 0 ,0,1)
from
mpintranet.zzzq_taobao_end_3 n
left join
mpintranet.mjw_qut s
on n.daterange = s.daterange and n.itemid = s.itemid
left join
day_sum t
on n.itemid = t.itemid
where n.daterange = '2016-12-15';





-------------------2016-12-16 日数据 -------------------

with day_sum as
(
select
itemid,
sum(SalesQty) as qut,
if(sum(isnew)=0,0,1) as isnew
from
mpintranet.day_qut
where daterange BETWEEN '2016-11-24' and '2016-12-15'
group by itemid
)

insert into mpintranet.day_qut
select
n.itemid,
n.shopid,
n.MonthlySalesQty - nvl(s.qut,0) - nvl(t.qut,0),
n.daterange,
n.DiscountPrice,
n.DiscountPrice2,
n.erro_info,
n.platform,
if(s.itemid is null and nvl(t.isnew ,0) = 0 ,0,1)
from
mpintranet.zzzq_taobao_end_3 n
left join
mpintranet.mjw_qut s
on n.daterange = s.daterange and n.itemid = s.itemid
left join
day_sum t
on n.itemid = t.itemid
where n.daterange = '2016-12-16';




------------------------------------------------------------------------------------29天之后的----------------------------------------------------------------------

with day_sum as
(
select
itemid,
sum(SalesQty) as qut,
if(sum(isnew)=0,0,1) as isnew
from
mpintranet.day_qut
where daterange BETWEEN date_sub('2016-12-23', 29) and date_sub('2016-12-23', 1)
group by itemid
)

insert into mpintranet.day_qut
select
n.itemid,
n.shopid,
n.MonthlySalesQty - nvl(t.qut,0),
n.daterange,
n.DiscountPrice,
n.DiscountPrice2,
n.erro_info,
n.platform,
if(s.itemid is null and nvl(t.isnew ,0) = 0 ,0,1)
from
mpintranet.zzzq_taobao_end_3 n
left join
day_sum t
on n.itemid = t.itemid
where n.crawldate = '2016-12-23';

------------------------------------------------------------------------------------查询数据----------------------------------------------------------------------


-- 6、输出数据
-- insert overwrite local directory "/home/data/tmp/zzzq"
-- row format delimited
-- fields terminated by "\t"

select
d.shopid,
d.itemid as "商品id",
d.daterange as "日期",
s.SalesAmt as "mjw-销售金额",
s.SalesQty as "mjw-销量",
s.DiscountPrice as "mjw-折扣价单价",
d.SalesQty as "销量",
d.DiscountPrice as "单价",
d.DiscountPrice2 as "sku单价",
d.SalesQty * d.DiscountPrice as "销售金额",
d.erro_info as "error_info",
d.platform,
d.isnew as "商品是否已存在"
from
mpintranet.day_qut d
left join
elengjing.women_clothing_item s
on s.itemid = d.itemid and s.daterange = d.daterange
where
d.daterange in ("2016-12-09","2016-12-10","2016-12-11");




select daterange,count(1) from mpintranet.day_qut  group by daterange  order by cast(daterange as date) desc ;








