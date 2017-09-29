

------------------------------------------------双十一女装品类排名-------------------------------------------------------------
select
categoryname,
platform,
sum(case when DateRange BETWEEN '2015-10-01' AND '2015-10-31' then SalesAmt else 0 end ) as "2015年10月各品类销售额",
sum(case when DateRange BETWEEN '2016-10-01' AND '2016-10-31' then SalesAmt else 0 end ) as "2016年10月各品类销售额",
sum(case when DateRange='2015-11-11' then SalesAmt else 0 end ) as "2015年双十一各品类销售额",
sum(case when DateRange='2016-11-11' then SalesAmt else 0 end ) as "2016年双十一各品类销售额"
from
elengjing.women_clothing_item 
where  
DateRange BETWEEN '2015-10-01' AND '2015-10-31' 
or DateRange BETWEEN '2016-10-01' AND '2016-10-31' 
or DateRange="2015-11-11"
or DateRange="2016-11-11"
group by categoryname,platform;




------------------------------------------------增长率大、绝对体量大的店铺-------------------------------------------------------------

with base as
(
select
shopid,
max(shopname) as shopname,
platform,
sum(case when DateRange BETWEEN '2015-01-01' AND '2015-10-31' then SalesAmt else 0 end ) as "oldyeary",
sum(case when DateRange BETWEEN '2016-01-01' AND '2016-10-31' then SalesAmt else 0 end ) as "newyear",
sum(case when DateRange='2015-11-11' then SalesAmt else 0 end ) as "2015_double_11",
sum(case when DateRange='2016-11-11' then SalesAmt else 0 end ) as "2016_double_11"
from
elengjing.women_clothing_item
where  
DateRange BETWEEN '2015-01-01' AND '2015-10-31' 
or DateRange BETWEEN '2016-01-01' AND '2016-10-31'
or DateRange="2015-11-11"
or DateRange="2016-11-11"
group by shopid,platform
)
select 
shopid as "店铺id",
shopname as "店铺名",
platform as "平台",
2015_double_11 as "2015年双11销售金额",
2016_double_11 as "2016年双11销售金额",
oldyeary as "2015年1-10月销售金额总和",
newyear as "2016年1-10月销售金额总和",
newyear/oldyeary as "增长率"
from base  order by 2016_double_11 desc ;


(162103,50000697,50008899,50010850,50013194)
('羽绒服','毛呢外套','毛衣','毛针织衫','连衣裙')




57303596


1520 1131
15201131

select sum(SalesAmt) from elengjing.women_clothing_item  where shopid = 57303596 and platform = "天猫" and  DateRange="2016-11-11";

select sum(SalesAmt) from elengjing.women_clothing_item  where shopid = 57303596  and  DateRange="2016-11-11";

select sum(SalesAmt) from e_elengjing.women_clothing_item  where shopid = 57303596  and  DateRange="2016-11-11";



------------------------------------------------件单价变化-------------------------------------------------------------


with base as
(
select
categoryname,
platform,
sum(case when DateRange BETWEEN '2015-10-01' AND '2015-10-31' then SalesAmt else 0 end ) as "oldyear_amt",
sum(case when DateRange BETWEEN '2016-10-01' AND '2016-10-31' then SalesAmt else 0 end ) as "newyear_amt",
sum(case when DateRange='2015-11-11' then SalesAmt else 0 end ) as "2015_oldyear11_amt",
sum(case when DateRange='2016-11-11' then SalesAmt else 0 end ) as "2016_oldyear11_amt",

sum(case when DateRange BETWEEN '2015-10-01' AND '2015-10-31' then SalesQty else 0 end ) as "oldyear_qty",
sum(case when DateRange BETWEEN '2016-10-01' AND '2016-10-31' then SalesQty else 0 end ) as "newyear_qty",
sum(case when DateRange='2015-11-11' then SalesQty else 0 end ) as "2015_oldyear11_qty",
sum(case when DateRange='2016-11-11' then SalesQty else 0 end ) as "2016_oldyear11_qty"
from
elengjing.women_clothing_item 
where  
DateRange BETWEEN '2015-10-01' AND '2015-10-31' 
or DateRange BETWEEN '2016-10-01' AND '2016-10-31' 
or DateRange="2015-11-11"
or DateRange="2016-11-11"
group by categoryname,platform
)
select 
categoryname,
platform,
oldyear_amt/oldyear_qty as "2015年10月件单价",
newyear_amt/newyear_qty as "2016年10月件单价",
2015_oldyear11_amt/2015_oldyear11_qty as "2015双十一当天件单价",
2016_oldyear11_amt/2016_oldyear11_qty as "2016双十一当天件单价"
from base;







select count(1) from elengjing.women_clothing_item where platform is null;




-------------------------------------------da D--------------------------------------------

with base as
(
select
DateRange,
categoryname,
platform,
sum(SalesAmt) as "amt",
sum(SalesQty) as "qty"
from
elengjing.women_clothing_item 
where  
DateRange BETWEEN '2016-10-21' AND '2016-11-09' 
or DateRange BETWEEN '2015-10-13' AND '2015-11-20' 
group by DateRange,categoryname,platform
)
select 
DateRange,
categoryname,
platform,
amt as "销售总额",
amt/qty as "件单价"
from base;


------------------------------------------------属性销售金额统计表------------------------------------------------------------




drop table elengjing.e_yuce_tmp;
CREATE TABLE elengjing.e_yuce_tmp (
categoryname STRING,
AttrName STRING,
AttrValue STRING,
platform STRING,
2015_double_11 STRING,
2016_double_11 STRING,
amt_1 STRING,
amt_2 STRING,
amt_3 STRING,
amt_4 STRING
)
STORED AS ORC;


insert into table elengjing.e_yuce_tmp
with attr as
(
select 
* 
from 
elengjing.women_clothing_item_attr
where AttrName in ("衣门襟","毛线粗细","适用年龄","裙长","裙型","款式","组合形式","领型","颜色分类","腰型","风格","图案","成分含量","穿着方式","上市年份季节","里料图案","功能","适用季节","含绒量","衣长","填充物","面料","街头","充绒量","材质","服装款式细节","袖长","服装版型","颜色组","厚薄","甜美","袖型","通勤","里料材质","廓形","尺码","面料材质")
), item as (
select 
ItemID,
SalesAmt,
DateRange,
categoryname,
CategoryID,
platform
from 
elengjing.women_clothing_item 
where CategoryID in (162103,50000697,50008899,50010850,50013194)
and (
DateRange BETWEEN '2016-09-01' AND '2016-10-31' 
or DateRange BETWEEN '2015-09-01' AND '2015-10-31' 
or DateRange="2015-11-11"
or DateRange="2016-11-11"
)
)
select
max(it.categoryname) as "品类名",
at.AttrName as "属性名",
at.AttrValue as "属性值",
it.platform,
sum(case when it.DateRange='2015-11-11' then it.SalesAmt else 0 end ) as "2015_double_11",
sum(case when it.DateRange='2016-11-11' then it.SalesAmt else 0 end ) as "2016_double_11",
sum(case when it.DateRange BETWEEN '2015-09-01' AND '2015-09-30' then it.SalesAmt else 0 end ) as "2015年9月销售金额",
sum(case when it.DateRange BETWEEN '2015-10-01' AND '2015-10-31' then it.SalesAmt else 0 end ) as "2015年10月销售金额",
sum(case when it.DateRange BETWEEN '2016-09-01' AND '2016-09-30' then it.SalesAmt else 0 end ) as "2016年9月销售金额",
sum(case when it.DateRange BETWEEN '2016-10-01' AND '2016-10-31' then it.SalesAmt else 0 end ) as "2016年10月销售金额"

from
attr at
left join item it
on at.itemid=it.itemid
group by it.CategoryID,at.AttrName,at.AttrValue,it.platform;



select 
categoryname  as "品类名",
attrname     as "属性名",
attrvalue    as "属性值",
platform     as "平台",
2015_double_11  as "2015年双11销售金额",
2016_double_11  as "2016年双11销售金额",
amt_1  as "2015年9月销售金额",
amt_2  as "2015年10月销售金额",
amt_3  as "2016年9月销售金额",
amt_4 as "2016年10月销售金额"
from 
elengjing.e_yuce_tmp;






