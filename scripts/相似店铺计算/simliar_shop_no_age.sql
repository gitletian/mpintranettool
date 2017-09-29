-- 1、 各店铺销售额排名表： 店铺ID  行业排名

-- select
-- t.*,
-- ROW_NUMBER() OVER (order by t.sum_salesAmt desc) as cn
-- from
-- (
-- select shopid, sum(SalesAmt) as sum_salesAmt from elengjing.women_clothing_item where platformid in (7001, 7002)  group by shopid order by sum_salesAmt desc
-- ) t
-- limit 100
-- ;


-- 2、 各店铺SPU数、销售额、销量、平均件单价统计表： 店铺ID  SPU数  销售额  销量   平均件单价

drop table if exists mpintranet.similar_shop_shop_1;
CREATE TABLE  if not exists mpintranet.similar_shop_shop_1(
 shopid   STRING
,platformid bigint
,spu   STRING
,salesQty STRING
,SalesAmt STRING
,avg_price STRING
)
CLUSTERED BY (shopid) INTO 113 BUCKETS
STORED AS ORC
;


insert into table mpintranet.similar_shop_shop_1
select
t.shopid,
max(platformid) as platformid,
count(1) as spu,
sum(t.SalesQty) as salesQty,
sum(t.SalesAmt) as SalesAmt,
nvl(round(sum(t.SalesAmt) / sum(t.SalesQty), 2), 0) as avg_price
from
(
    select
    shopid,
    itemid,
    max(platformid) as platformid,
    sum(SalesQty) as SalesQty,
    sum(SalesAmt) as SalesAmt
    from elengjing.women_clothing_item
    group by shopid,itemid
) t
group by t.shopid
;


-- 3、 各店铺各品类SPU数：店铺ID  品类1  品类2 。。。  品类n
-- 4、 各店铺各品类销售额(销量)：店铺ID  品类1  品类2 。。。  品类n
-- 5、 各店铺各品类销售量：店铺ID  品类1  品类2 。。。  品类n
drop table if exists mpintranet.similar_shop_category_1;
CREATE TABLE  if not exists mpintranet.similar_shop_category_1(
 shopid   STRING
,Categoryname   STRING
,spu STRING
,SalesAmt STRING
,SalesQty STRING
)
CLUSTERED BY (shopid) INTO 113 BUCKETS
STORED AS ORC
;


with  similar_shop_category as(
    select
    shopid,
    Categoryname,
    count(distinct itemid) as spu,
    sum(SalesAmt) as SalesAmt,
    sum(SalesQty) as SalesQty
    from
    elengjing.women_clothing_item
    group by shopid, Categoryname
),
similar_shop_shopid_category as (
    select
    s.shopid,
    c.Categoryname
    from elengjing.category c
    cross join
    elengjing.shop  s
),
similar_shop_category_join as (
    select
    c1.shopid,
    c1.Categoryname,
    nvl(c2.spu, 0) as spu,
    nvl(c2.SalesAmt, 0) as SalesAmt,
    nvl(c2.SalesQty, 0) as SalesQty
    from
    similar_shop_shopid_category c1
    left join
    similar_shop_category c2
    on c1.shopid = c2.shopid and c1.Categoryname = c2.Categoryname
)

insert into table mpintranet.similar_shop_category_1

select
shopid,
concat_ws(",", collect_list(Categoryname)) as Categoryname,
concat_ws(",", collect_list(cast(spu as String))) as spu,
concat_ws(",", collect_list(cast(salesamt as String))) as salesamt,
concat_ws(",", collect_list(cast(salesqty as String))) as salesqty
from
(
select
shopid,
Categoryname,
spu,
salesamt,
salesqty
from
similar_shop_category_join
DISTRIBUTE BY shopid
SORT BY Categoryname desc
) t
group by shopid
;


-- 6、 各店铺各风格SPU数分布：店铺ID 风格1  风格2 。。。 风格n             == 风格   中老年风格
--- similar_shop_attr_style  风格

drop table if exists mpintranet.similar_shop_attr_style_1;
CREATE TABLE  if not exists mpintranet.similar_shop_attr_style_1(
 shopid   STRING
,attrvalue   STRING
,spu STRING
)
CLUSTERED BY (shopid) INTO 113 BUCKETS
STORED AS ORC
;


with shopid_attr_spu as (
    select
    shopid,
    fengge as attrvalue,
    count(distinct itemid) as spu
    from
    elengjing.women_clothing_item_attr
    where  fengge is not null
    group by shopid, fengge
),
attr_list as (
    select
    fengge as attrvalue
    from
    elengjing.women_clothing_item_attr
    where fengge is not null
    group by fengge
),
full_shopid_attr as (
    select
    t2.shopid,
    t1.attrvalue
    from
    attr_list t1
    cross join
    elengjing.shop  t2
),
shopid_attr_join as (
    select
    a.shopid,
    a.attrvalue,
    nvl(s.spu, 0) as spu
    from
    full_shopid_attr a
    left join
    shopid_attr_spu s
    on a.shopid = s.shopid and a.attrvalue = s.attrvalue
)

insert into table mpintranet.similar_shop_attr_style_1
select
shopid,
concat_ws(",", collect_list(attrvalue)) as attrvalue,
concat_ws(",", collect_list(cast(spu as String))) as spu
from
(
select
*
from
shopid_attr_join
DISTRIBUTE BY shopid
SORT BY attrvalue desc
) t
group by shopid
;

-------------------------------------------------------------------------------------------------------------------------------------
-- 二、 合并所有的 数据
--  7001, 7002 淘宝   7011 7012 天猫
-- 73401272 天猫    57130130 淘宝      67001977

drop table if exists mpintranet.similar_shop_data_1;
CREATE TABLE  if not exists mpintranet.similar_shop_data_1(
 shopid   STRING

,s_shop_spu   STRING
,s_shop_salesQty STRING
,s_shop_SalesAmt   STRING
,s_shop_avg_price STRING

,s_category_spu   STRING
,s_category_SalesQty STRING
,s_category_SalesAmt STRING

,s_age_spu   STRING
,s_style_spu STRING


,t_shop_spu   STRING
,t_shop_salesQty STRING
,t_shop_SalesAmt   STRING
,t_shop_avg_price STRING

,t_category_spu   STRING
,t_category_SalesQty STRING
,t_category_SalesAmt STRING

,t_age_spu   STRING
,t_style_spu STRING
)
CLUSTERED BY (shopid) INTO 113 BUCKETS
STORED AS ORC
;




with source_shop as (
    select
    if(shop.platformid in (7001, 7002), 7001, if(shop.platformid in (7011, 7012), 7011, null)) as platformid,
    shop.shopid,
    shop.spu as shop_spu,
    shop.salesQty as shop_salesQty,
    shop.SalesAmt as shop_SalesAmt,
    shop.avg_price as shop_avg_price,

    category.spu as category_spu,
    category.SalesQty as category_SalesQty,
    category.SalesAmt as category_SalesAmt,

    100 as age_spu,
    style.spu as style_spu
    from
    mpintranet.similar_shop_shop shop
    join
    mpintranet.similar_shop_category category
    on shop.shopid = category.shopid

    join
    mpintranet.similar_shop_attr_style style
    on shop.shopid = style.shopid
),
target_shop as (
    select
    if(t_shop.platformid in (7001, 7002), 7001, if(t_shop.platformid in (7011, 7012), 7011, null)) as platformid,
    t_shop.shopid,
    t_shop.spu as shop_spu,
    t_shop.salesQty as shop_salesQty,
    t_shop.SalesAmt as shop_SalesAmt,
    t_shop.avg_price as shop_avg_price,

    t_category.spu as category_spu,
    t_category.SalesQty as category_SalesQty,
    t_category.SalesAmt as category_SalesAmt,

    100 as age_spu,
    t_style.spu as style_spu
    from
    mpintranet.similar_shop_shop t_shop
    join
    mpintranet.similar_shop_category t_category
    on t_shop.shopid = t_category.shopid

    join
    mpintranet.similar_shop_attr_style t_style
    on t_shop.shopid = t_style.shopid
    where t_shop.shopid = '73401272'
)


insert into table mpintranet.similar_shop_data_1
select
s.shopid,

s.shop_spu,
s.shop_salesQty,
s.shop_SalesAmt,
s.shop_avg_price,

s.category_spu,
s.category_SalesQty,
s.category_SalesAmt,

s.age_spu,
s.style_spu,

t.shop_spu,
t.shop_salesQty,
t.shop_SalesAmt,
t.shop_avg_price,

t.category_spu,
t.category_SalesQty,
t.category_SalesAmt,

t.age_spu,
t.style_spu

from
source_shop s
left join
target_shop t
on s.platformid = t.platformid
where t.shopid is not null
;


-- 三、进行相似度计算
--- 第一步
drop table if exists mpintranet.similar_shop_sim_3;
CREATE TABLE  if not exists mpintranet.similar_shop_sim_3(
 shopid   STRING
,sim   STRING
,error_info STRING
)
CLUSTERED BY (shopid) INTO 113 BUCKETS
STORED AS ORC
;

add file /home/script/normal_servers/serverudf/elengjing/similar_shop_2.py;

insert into table mpintranet.similar_shop_sim_3
select
  TRANSFORM (shopid, s_shop_spu, s_shop_salesqty, s_shop_salesamt, s_shop_avg_price, s_category_spu, s_category_salesqty, s_category_salesamt, s_age_spu, s_style_spu, t_shop_spu, t_shop_salesqty, t_shop_salesamt, t_shop_avg_price, t_category_spu, t_category_salesqty, t_category_salesamt, t_age_spu, t_style_spu)
  USING 'python similar_shop_2.py'
  AS (shopid, sim, error_info)
FROM
mpintranet.similar_shop_data_1
;










