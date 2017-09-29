--------------------------------------------------------------------------------------
----------                   根据 mjw 和自主抓取的月销量,计算出 日销量(使用于前30天数据)
----------1、BEGIN_DATE: 开始时间
----------2、END_DATE
----------3、CURRENT_DATE
--------------------------------------------------------------------------------------
with day_sum as
(
select
itemid,
sum(SalesQty) as qut,
if(sum(isnew)=0,0,1) as isnew
from
mpintranet.day_qut
where daterange BETWEEN '${hivevar:BEGIN_DATE}' and '${hivevar:END_DATE}'
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
where n.crawldate = '${hivevar:CURRENT_DAY}';
