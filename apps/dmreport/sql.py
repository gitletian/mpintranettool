# coding: utf-8
# __author__: ""
from __future__ import unicode_literals


ITEM_MONTH_RAGEZB = '''
with sycm_item_month as
(
select
shopid,
itemid,
max(shopname) as shopname,
sum(paymonay) as SalesAmt,
sum(payitemjianshu) as SalesQty,
daterange,
sum(paymonay) / sum (payitemjianshu)  as av_price
from
mpintranet.sycm_item
where
datatype = 2
and
shopid in ({shop_ids})
and
daterange between "{min_date}" and "{max_date}"
group by shopid,daterange,itemid
),
 mjw_item_month as
(
select
shopid,
itemid,
sum(SalesAmt) as SalesAmt,
sum(SalesQty) as SalesQty,
substr(daterange, 0, 7) as daterange,
sum(SalesAmt) / sum (SalesQty) as av_price
from
{compare_table}
where
shopid in ({shop_ids})
and
daterange between "{min_date}-01" and date_sub(add_months('{max_date}-01',1), 1)
and
categoryid = {category_id}
group by shopid,substr(daterange, 0, 7),itemid
),

disc_rate as
(
select
sycm.shopid,
sycm.shopname,
sycm.itemid,
sycm.daterange,
mjw.av_price / sycm.av_price -1 as av_price_disc_rate,
mjw.SalesQty / sycm.SalesQty -1 as salesqty_disc_rate,
mjw.SalesAmt / sycm.SalesAmt -1 as salesmmt_disc_rate
from
sycm_item_month sycm
inner join
mjw_item_month mjw
on sycm.shopid = mjw.shopid and sycm.daterange = mjw.daterange and sycm.itemid = mjw.itemid
),

disc_rate2 as
(
select
shopid,
max(shopname) as shopname,
count(if(av_price_disc_rate >= {min_rate} and av_price_disc_rate <= {max_rate},1 , null)) as av_price_cnt,
sum(if(av_price_disc_rate >= {min_rate} and av_price_disc_rate <= {max_rate}, av_price_disc_rate,0)) as av_price_sm,
nvl(min(if(av_price_disc_rate >= {min_rate} and av_price_disc_rate <= {max_rate}, av_price_disc_rate,null)), 0) as av_price_mn,
nvl(max(if(av_price_disc_rate >={min_rate} and av_price_disc_rate <= {max_rate}, av_price_disc_rate,null)), 0) as av_price_mx,

count(if(salesqty_disc_rate >= {min_rate} and salesqty_disc_rate <= {max_rate}, 1, null)) as salesqty_cnt,
sum(if(salesqty_disc_rate >= {min_rate} and salesqty_disc_rate <= {max_rate}, salesqty_disc_rate,0)) as salesqty_sm,
nvl(min(if(salesqty_disc_rate >= {min_rate} and salesqty_disc_rate <= {max_rate}, salesqty_disc_rate,null)), 0) as salesqty_mn,
nvl(max(if(salesqty_disc_rate >= {min_rate} and salesqty_disc_rate <= {max_rate}, salesqty_disc_rate,null)), 0) as salesqty_mx,

count(if(salesmmt_disc_rate >= {min_rate} and salesmmt_disc_rate <= {max_rate}, 1,null)) as salesmmt_cnt,
sum(if(salesmmt_disc_rate >= {min_rate} and salesmmt_disc_rate <= {max_rate}, salesmmt_disc_rate,0)) as salesmmt_sm,
nvl(min(if(salesmmt_disc_rate >= {min_rate} and salesmmt_disc_rate <= {max_rate}, salesmmt_disc_rate,null)), 0) as salesmmt_mn,
nvl(max(if(salesmmt_disc_rate >= {min_rate} and salesmmt_disc_rate <= {max_rate}, salesmmt_disc_rate,null)), 0) as salesmmt_mx

from
disc_rate
group by
shopid
)

select
shopid,
shopname,
if(av_price_cnt>=2, (nvl(av_price_sm, 0) - nvl(av_price_mn, 0) - nvl(av_price_mx, 0)) / if(av_price_cnt -2>0, av_price_cnt, 1), nvl(av_price_sm,0)) * 100 as av_price_disc_rate,
if(salesqty_cnt>=2, (nvl(salesqty_sm, 0) - nvl(salesqty_mn, 0) - nvl(salesqty_mx, 0)) / if(salesqty_cnt -2>0, salesqty_cnt, 1), nvl(salesqty_sm,0)) * 100 as salesqty_disc_rate,
if(salesmmt_cnt>=2, (nvl(salesmmt_sm, 0) - nvl(salesmmt_mn, 0) - nvl(salesmmt_mx, 0)) / if(salesmmt_cnt -2>0, salesmmt_cnt, 1), nvl(salesmmt_sm,0)) * 100 as salesmmt_disc_rate
from disc_rate2
'''


ITEM_DAY_RAGEZB = '''
with sycm_item_day as
(
select
shopid,
itemid,
max(shopname) as shopname,
sum(paymonay) as SalesAmt,
sum(payitemjianshu) as SalesQty,
daterange,
sum(paymonay) / sum (payitemjianshu)  as av_price
from
mpintranet.sycm_item
where
datatype = 1
and
shopid in ({shop_ids})
and
daterange in ('{dates}')
group by shopid,daterange,itemid
),
 mjw_item_day as
(
select
shopid,
itemid,
sum(SalesAmt) as SalesAmt,
sum(SalesQty) as SalesQty,
daterange,
sum(SalesAmt) / sum (SalesQty) as av_price
from
{compare_table}
where
shopid in ({shop_ids})
and
daterange in ('{dates}')
and
categoryid = {category_id}
group by shopid,daterange,itemid
),
disc_rate as
(
select
sycm.shopid,
sycm.shopname,
sycm.daterange,
mjw.av_price / sycm.av_price -1 as av_price_disc_rate,
mjw.SalesQty / sycm.SalesQty -1 as salesqty_disc_rate,
mjw.SalesAmt / sycm.SalesAmt -1 as salesmmt_disc_rate
from
sycm_item_day sycm
inner join
mjw_item_day mjw
on sycm.shopid = mjw.shopid and sycm.daterange = mjw.daterange and sycm.itemid = mjw.itemid
),
disc_rate2 as
(
select
shopid,
max(shopname) as shopname,
count(if(av_price_disc_rate >= {min_rate} and av_price_disc_rate <= {max_rate},1 , null)) as av_price_cnt,
sum(if(av_price_disc_rate >= {min_rate} and av_price_disc_rate <= {max_rate}, av_price_disc_rate,0)) as av_price_sm,
nvl(min(if(av_price_disc_rate >= {min_rate} and av_price_disc_rate <= {max_rate}, av_price_disc_rate,null)), 0) as av_price_mn,
nvl(max(if(av_price_disc_rate >={min_rate} and av_price_disc_rate <= {max_rate}, av_price_disc_rate,null)), 0) as av_price_mx,

count(if(salesqty_disc_rate >= {min_rate} and salesqty_disc_rate <= {max_rate}, 1, null)) as salesqty_cnt,
sum(if(salesqty_disc_rate >= {min_rate} and salesqty_disc_rate <= {max_rate}, salesqty_disc_rate,0)) as salesqty_sm,
nvl(min(if(salesqty_disc_rate >= {min_rate} and salesqty_disc_rate <= {max_rate}, salesqty_disc_rate,null)), 0) as salesqty_mn,
nvl(max(if(salesqty_disc_rate >= {min_rate} and salesqty_disc_rate <= {max_rate}, salesqty_disc_rate,null)), 0) as salesqty_mx,

count(if(salesmmt_disc_rate >= {min_rate} and salesmmt_disc_rate <= {max_rate}, 1,null)) as salesmmt_cnt,
sum(if(salesmmt_disc_rate >= {min_rate} and salesmmt_disc_rate <= {max_rate}, salesmmt_disc_rate,0)) as salesmmt_sm,
nvl(min(if(salesmmt_disc_rate >= {min_rate} and salesmmt_disc_rate <= {max_rate}, salesmmt_disc_rate,null)), 0) as salesmmt_mn,
nvl(max(if(salesmmt_disc_rate >= {min_rate} and salesmmt_disc_rate <= {max_rate}, salesmmt_disc_rate,null)), 0) as salesmmt_mx

from
disc_rate
group by
shopid
)

select
shopid,
shopname,
if(av_price_cnt>=2, (nvl(av_price_sm, 0) - nvl(av_price_mn, 0) - nvl(av_price_mx, 0)) / if(av_price_cnt -2>0, av_price_cnt, 1), nvl(av_price_sm,0)) * 100 as av_price_disc_rate,
if(salesqty_cnt>=2, (nvl(salesqty_sm, 0) - nvl(salesqty_mn, 0) - nvl(salesqty_mx, 0)) / if(salesqty_cnt -2>0, salesqty_cnt, 1), nvl(salesqty_sm,0)) * 100 as salesqty_disc_rate,
if(salesmmt_cnt>=2, (nvl(salesmmt_sm, 0) - nvl(salesmmt_mn, 0) - nvl(salesmmt_mx, 0)) / if(salesmmt_cnt -2>0, salesmmt_cnt, 1), nvl(salesmmt_sm,0)) * 100 as salesmmt_disc_rate
from disc_rate2
'''


ITEM_MONTH_RAGE = '''
with sycm_item_month as
(
select
shopid,
itemid,
max(shopname) as shopname,
sum(paymonay) as SalesAmt,
sum(payitemjianshu) as SalesQty,
daterange,
sum(paymonay) / sum (payitemjianshu)  as av_price
from
mpintranet.sycm_item
where
datatype = 2
and
shopid in ({shop_ids})
and
daterange between "{min_date}" and "{max_date}"
and
itemid in (40501886050,40502650048,41555559880,42635964085)
group by shopid,daterange,itemid
),
 mjw_item_month as
(
select
shopid,
itemid,
sum(SalesAmt) as SalesAmt,
sum(SalesQty) as SalesQty,
substr(daterange, 0, 7) as daterange,
sum(SalesAmt) / sum (SalesQty) as av_price
from
{compare_table}
where
shopid in ({shop_ids})
and
daterange between "{min_date}-01" and date_sub(add_months('{max_date}-01',1), 1)
and
categoryid = {category_id}
and
itemid in (40501886050,40502650048,41555559880,42635964085)
group by shopid,substr(daterange, 0, 7),itemid
)

select
sycm.shopid,
sycm.shopname,
sycm.itemid,
sycm.daterange,
(mjw.av_price / sycm.av_price -1) * 100 as av_price_disc_rate,
(mjw.SalesQty / sycm.SalesQty -1) * 100 as salesqty_disc_rate,
(mjw.SalesAmt / sycm.SalesAmt -1) * 100 as salesmmt_disc_rate
from
sycm_item_month sycm
inner join
mjw_item_month mjw
on sycm.shopid = mjw.shopid and sycm.daterange = mjw.daterange and sycm.itemid = mjw.itemid order by sycm.daterange ASC


'''


ITEM_DAY_RAGE = '''
with sycm_item_day as
(
select
shopid,
itemid,
max(shopname) as shopname,
sum(paymonay) as SalesAmt,
sum(payitemjianshu) as SalesQty,
daterange,
sum(paymonay) / sum (payitemjianshu)  as av_price
from
mpintranet.sycm_item
where
datatype = 1
and
shopid in ({shop_ids})
and
daterange in ('{dates}')
and
itemid in (538785147805,536273883940,538891278909,527159453727)
group by shopid,daterange,itemid
),
 mjw_item_day as
(
select
shopid,
itemid,
sum(SalesAmt) as SalesAmt,
sum(SalesQty) as SalesQty,
daterange,
sum(SalesAmt) / sum (SalesQty) as av_price
from
{compare_table}
where
shopid in ({shop_ids})
and
daterange in ('{dates}')
and
categoryid = {category_id}
and
itemid in (538785147805,536273883940,538891278909,527159453727)
group by shopid,daterange,itemid
)

select
sycm.shopid,
sycm.shopname,
mjw.itemid,
sycm.daterange,
(mjw.av_price / sycm.av_price -1) * 100 as av_price_disc_rate,
(mjw.SalesQty / sycm.SalesQty -1) * 100 as salesqty_disc_rate,
(mjw.SalesAmt / sycm.SalesAmt -1) * 100 as salesmmt_disc_rate
from
sycm_item_day sycm
inner join
mjw_item_day mjw
on sycm.shopid = mjw.shopid and sycm.daterange = mjw.daterange and sycm.itemid = mjw.itemid


'''


# 店铺层级
SHOP_MONTH_RAGEZB = '''
with sycm_item_month as
(
select
shopid,
max(shopname) as shopname,
sum(paymonay) as SalesAmt,
sum(payitemjianshu) as SalesQty,
daterange,
count(distinct if(payitemjianshu>0,itemid,null)) as spu,
sum(paymonay) / sum (payitemjianshu)  as av_price
from
mpintranet.sycm_item
where
datatype = 2
and
shopid in ({shop_ids})
and
daterange between "{min_date}" and "{max_date}"
group by shopid,daterange
),
 mjw_item_month as
(
select
shopid,
sum(SalesAmt) as SalesAmt,
sum(SalesQty) as SalesQty,
substr(daterange, 0, 7) as daterange,
count(distinct if(SalesQty>0,itemid,null)) as spu,
sum(SalesAmt) / sum (SalesQty) as av_price
from
{compare_table}
where
shopid in ({shop_ids})
and
daterange between "{min_date}-01" and date_sub(add_months('{max_date}-01',1), 1)
group by shopid,substr(daterange, 0, 7)
),
disc_rate as
(
select
sycm.shopid,
sycm.shopname,
sycm.daterange,
mjw.av_price / sycm.av_price -1 as av_price_disc_rate,
mjw.spu / sycm.spu -1 as spu_disc_rate,
mjw.SalesQty / sycm.SalesQty -1 as salesqty_disc_rate,
mjw.SalesAmt / sycm.SalesAmt -1 as salesmmt_disc_rate
from
sycm_item_month sycm
left join
mjw_item_month mjw
on sycm.shopid = mjw.shopid and sycm.daterange = mjw.daterange
)

select
shopid,
max(shopname) as shopname,
count(if(av_price_disc_rate >= {min_rate} and av_price_disc_rate <= {max_rate}, 1, null)) / {date_len} * 100 as av_price_disc_rate,
count(if(spu_disc_rate >= {min_rate} and spu_disc_rate <= {max_rate}, 1, null)) / {date_len} * 100 as spu_disc_rate,
count(if(salesqty_disc_rate >= {min_rate} and salesqty_disc_rate <= {max_rate},1, null)) / {date_len} * 100 as salesqty_disc_rate,
count(if(salesmmt_disc_rate >= {min_rate} and salesmmt_disc_rate <= {max_rate}, 1, null)) / {date_len} * 100 as salesmmt_disc_rate
from
disc_rate group by shopid;

'''


SHOP_DAY_RAGEZB = '''
with sycm_item_day as
(
select
shopid,
max(shopname) as shopname,
sum(paymonay) as SalesAmt,
sum(payitemjianshu) as SalesQty,
daterange,
count(distinct if(payitemjianshu>0,itemid,null)) as spu,
sum(paymonay) / sum (payitemjianshu)  as av_price
from
mpintranet.sycm_item
where
datatype = 1
and
shopid in ({shop_ids})
and
daterange in ('{dates}')
group by shopid,daterange
),
 mjw_item_day as
(
select
shopid,
sum(SalesAmt) as SalesAmt,
sum(SalesQty) as SalesQty,
daterange,
count(distinct if(SalesQty>0,itemid,null)) as spu,
sum(SalesAmt) / sum (SalesQty) as av_price
from
{compare_table}
where
shopid in ({shop_ids})
and
daterange in ('{dates}')
group by shopid,daterange
),
disc_rate as
(
select
sycm.shopid,
sycm.shopname as shopname,
sycm.daterange,
mjw.av_price / sycm.av_price -1 as av_price_disc_rate,
mjw.spu / sycm.spu -1 as spu_disc_rate,
mjw.SalesQty / sycm.SalesQty -1 as salesqty_disc_rate,
mjw.SalesAmt / sycm.SalesAmt -1 as salesmmt_disc_rate
from
sycm_item_day sycm
left join
mjw_item_day mjw
on sycm.shopid = mjw.shopid and sycm.daterange = mjw.daterange
)

select
shopid,
max(shopname) as shopname,
count(if(av_price_disc_rate >= {min_rate} and av_price_disc_rate <= {max_rate}, 1, null)) / {date_len} * 100 as av_price_disc_rate,
count(if(spu_disc_rate >= {min_rate} and spu_disc_rate <= {max_rate}, 1, null)) / {date_len} * 100 as spu_disc_rate,
count(if(salesqty_disc_rate >= {min_rate} and salesqty_disc_rate <= {max_rate},1, null)) / {date_len} * 100 as salesqty_disc_rate,
count(if(salesmmt_disc_rate >= {min_rate} and salesmmt_disc_rate <= {max_rate}, 1, null)) / {date_len} * 100 as salesmmt_disc_rate

from
disc_rate group by shopid;

'''


SHOP_MONTH_RAGE = '''
with sycm_item_month as
(
select
shopid,
sum(paymonay) as SalesAmt,
sum(payitemjianshu) as SalesQty,
daterange,
count(distinct if(payitemjianshu>0,itemid,null)) as spu,
sum(paymonay) / sum (payitemjianshu)  as av_price
from
mpintranet.sycm_item
where
datatype = 2
and
shopid in ({shop_ids})
and
daterange between "{min_date}" and "{max_date}"
group by shopid,daterange
),
 mjw_item_month as
(
select
shopid,
sum(SalesAmt) as SalesAmt,
sum(SalesQty) as SalesQty,
substr(daterange, 0, 7) as daterange,
count(distinct if(SalesQty>0,itemid,null)) as spu,
sum(SalesAmt) / sum (SalesQty) as av_price
from
{compare_table}
where
shopid in ({shop_ids})
and
daterange between "{min_date}-01" and date_sub(add_months('{max_date}-01',1), 1)
group by shopid,substr(daterange, 0, 7)
)
select
sycm.shopid,
sycm.daterange,
(mjw.av_price / sycm.av_price -1) * 100 as av_price_disc_rate,
(mjw.spu / sycm.spu -1) * 100 as spu_disc_rate,
(mjw.SalesQty / sycm.SalesQty -1) * 100 as salesqty_disc_rate,
(mjw.SalesAmt / sycm.SalesAmt -1) * 100 as salesmmt_disc_rate
from
sycm_item_month sycm
left join
mjw_item_month mjw
on sycm.shopid = mjw.shopid and sycm.daterange = mjw.daterange

'''


SHOP_DAY_RAGE = '''
with sycm_item_day as
(
select
shopid,
sum(paymonay) as SalesAmt,
sum(payitemjianshu) as SalesQty,
daterange,
count(distinct if(payitemjianshu>0,itemid,null)) as spu,
sum(paymonay) / sum (payitemjianshu)  as av_price
from
mpintranet.sycm_item
where
datatype = 1
and
shopid in ({shop_ids})
and
daterange in ('{dates}')
group by shopid,daterange
),
 mjw_item_day as
(
select
shopid,
sum(SalesAmt) as SalesAmt,
sum(SalesQty) as SalesQty,
daterange,
count(distinct if(SalesQty>0,itemid,null)) as spu,
sum(SalesAmt) / sum (SalesQty) as av_price
from
{compare_table}
where
shopid in ({shop_ids})
and
daterange in ('{dates}')
group by shopid,daterange
)

select
sycm.shopid,
sycm.daterange,
(mjw.av_price / sycm.av_price -1) * 100 as av_price_disc_rate,
(mjw.spu / sycm.spu -1) * 100 as spu_disc_rate,
(mjw.SalesQty / sycm.SalesQty -1) * 100 as salesqty_disc_rate,
(mjw.SalesAmt / sycm.SalesAmt -1) * 100 as salesmmt_disc_rate
from
sycm_item_day sycm
left join
mjw_item_day mjw
on sycm.shopid = mjw.shopid and sycm.daterange = mjw.daterange

'''


CATEGORY_MONTH_RAGE = '''
with sycm_item_day as
(
select
shopid,
daterange,
count(distinct if(payitemjianshu>0,itemid,null)) as spu,
sum(paymonay) as SalesAmt,
sum(payitemjianshu) as SalesQty,
sum(paymonay) / sum (payitemjianshu)  as av_price
from
mpintranet.sycm_item
where
datatype = 2
and
shopid in ({shop_ids})
and
daterange between "{min_date}" and "{max_date}"
and
payitemjianshu >= {min_qty}
group by shopid,daterange
),
 mjw_item_day as
(
select
max(categoryid) as categoryid,
shopid,
substr(daterange, 0, 7) as daterange,
count(distinct if(SalesQty>0,itemid,null)) as spu,
sum(SalesAmt) as SalesAmt,
sum(SalesQty) as SalesQty,
sum(SalesAmt) / sum (SalesQty) as av_price
from
{compare_table}
where
shopid in ({shop_ids})
and
daterange between "{min_date}-01" and date_sub(add_months('{max_date}-01',1), 1)
and SalesQty >= {min_qty}
{category_where}
group by shopid,substr(daterange, 0, 7)
)

select
sycm.shopid,
mjw.categoryid,
sycm.daterange,
sum(mjw.av_price) / sum(sycm.av_price) - 1 as av_price_disc_rate,
sum(mjw.spu) / sum(sycm.spu) - 1  as spu_disc_rate,
sum(mjw.SalesQty) / sum(sycm.SalesQty) - 1 as salesqty_disc_rate,
sum(mjw.SalesAmt) / sum(sycm.SalesAmt) - 1 as salesmmt_disc_rate
from
sycm_item_day sycm
left join
mjw_item_day mjw
on sycm.shopid = mjw.shopid and sycm.daterange = mjw.daterange
where
mjw.shopid is not null

group by sycm.shopid,mjw.categoryid,sycm.daterange;

'''


CATEGORY_MONTH_RAGE_SHOP = '''
with sycm_item_day as
(
select
shopid,
count(distinct if(payitemjianshu>0,itemid,null)) as spu,
sum(paymonay) as SalesAmt,
sum(payitemjianshu) as SalesQty,
daterange,
sum(paymonay) / sum (payitemjianshu)  as av_price
from
mpintranet.sycm_item
where
datatype = 2
and
shopid in ({shop_ids})
and
daterange between "{min_date}" and "{max_date}"
and
payitemjianshu >= {min_qty}
group by shopid,daterange
),
 mjw_item_day as
(
select
max(categoryid) as categoryid,
shopid,
count(distinct if(SalesQty>0,itemid,null)) as spu,
substr(daterange, 0, 7) as daterange,
sum(SalesAmt) as SalesAmt,
sum(SalesQty) as SalesQty,
sum(SalesAmt) / sum (SalesQty) as av_price
from
{compare_table}
where
shopid in ({shop_ids})
and
daterange between "{min_date}-01" and date_sub(add_months('{max_date}-01',1), 1)
and SalesQty >= {min_qty}
{category_where}
group by shopid,substr(daterange, 0, 7)
)

select
sycm.shopid,
mjw.categoryid,
sycm.daterange,
sum(mjw.av_price) / sum(sycm.av_price) - 1 as av_price_disc_rate,
sum(mjw.spu) / sum(sycm.spu) - 1  as spu_disc_rate,
sum(mjw.SalesQty) / sum(sycm.SalesQty) - 1 as salesqty_disc_rate,
sum(mjw.SalesAmt) / sum(sycm.SalesAmt) - 1 as salesmmt_disc_rate
from
sycm_item_day sycm
left join
mjw_item_day mjw
on sycm.shopid = mjw.shopid and sycm.daterange = mjw.daterange
where
mjw.shopid is not null

group by mjw.categoryid,sycm.shopid,sycm.daterange;
'''


CATEGORY_DAY_RAGE = '''
with sycm_item_day as
(
select
shopid,
daterange,
count(distinct if(payitemjianshu>0,itemid,null)) as spu,
sum(paymonay) as SalesAmt,
sum(payitemjianshu) as SalesQty,
sum(paymonay) / sum (payitemjianshu)  as av_price
from
mpintranet.sycm_item
where
datatype = 1
and
shopid in ({shop_ids})
and
daterange in ('{dates}')
and
payitemjianshu >= {min_qty}
group by shopid,daterange
),
 mjw_item_day as
(
select
max(categoryid) as categoryid,
shopid,
daterange,
count(distinct if(SalesQty>0,itemid,null)) as spu,
sum(SalesAmt) as SalesAmt,
sum(SalesQty) as SalesQty,
sum(SalesAmt) / sum (SalesQty) as av_price
from
{compare_table}
where
shopid in ({shop_ids})
and
daterange in ('{dates}')
and SalesQty >= {min_qty}
{category_where}
group by shopid,daterange
)

select
sycm.shopid,
mjw.categoryid,
sycm.daterange,
sum(mjw.av_price) / sum(sycm.av_price) - 1 as av_price_disc_rate,
sum(mjw.spu) / sum(sycm.spu) - 1  as spu_disc_rate,
sum(mjw.SalesQty) / sum(sycm.SalesQty) - 1 as salesqty_disc_rate,
sum(mjw.SalesAmt) / sum(sycm.SalesAmt) - 1 as salesmmt_disc_rate
from
sycm_item_day sycm
left join
mjw_item_day mjw
on sycm.shopid = mjw.shopid and sycm.daterange = mjw.daterange
where
mjw.shopid is not null

group by sycm.shopid,mjw.categoryid,sycm.daterange;
'''


CATEGORY_DAY_RAGE_SHOP = '''
with sycm_item_day as
(
select
shopid,
count(distinct if(payitemjianshu>0,itemid,null)) as spu,
sum(paymonay) as SalesAmt,
sum(payitemjianshu) as SalesQty,
daterange,
sum(paymonay) / sum (payitemjianshu)  as av_price
from
mpintranet.sycm_item
where
datatype = 1
and
shopid in ({shop_ids})
and
daterange in ('{dates}')
and
payitemjianshu >= {min_qty}
group by shopid,daterange
),
 mjw_item_day as
(
select
max(categoryid) as categoryid,
shopid,
count(distinct if(SalesQty>0,itemid,null)) as spu,
sum(SalesAmt) as SalesAmt,
sum(SalesQty) as SalesQty,
daterange,
sum(SalesAmt) / sum (SalesQty) as av_price
from
{compare_table}
where
shopid in ({shop_ids})
and
daterange in ('{dates}')
and SalesQty >= {min_qty}
{category_where}
group by shopid,daterange
)

select
sycm.shopid,
mjw.categoryid,
sycm.daterange,
sum(mjw.av_price) / sum(sycm.av_price) - 1 as av_price_disc_rate,
sum(mjw.spu) / sum(sycm.spu) - 1  as spu_disc_rate,
sum(mjw.SalesQty) / sum(sycm.SalesQty) - 1 as salesqty_disc_rate,
sum(mjw.SalesAmt) / sum(sycm.SalesAmt) - 1 as salesmmt_disc_rate
from
sycm_item_day sycm
left join
mjw_item_day mjw
on sycm.shopid = mjw.shopid and sycm.daterange = mjw.daterange
where
mjw.shopid is not null

group by mjw.categoryid,sycm.shopid,sycm.daterange;

'''