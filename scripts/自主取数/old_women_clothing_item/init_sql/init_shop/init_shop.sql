drop table if exists elengjing.shop_new;
---truncate table elengjing.shop_new;
CREATE TABLE if not exists elengjing.shop_new(
shopid BIGINT,
shopname STRING,
address STRING,
level INTEGER,
shop_url STRING,
favor BIGINT,
sellerid STRING,
nick STRING,
platform STRING,
dsr STRING
)
STORED AS ORC;


INSERT INTO table elengjing.shop_new
SELECT
shopid,
shopname,
address,
level,
shop_url,
favor,
sellerid,
nick,
platform,
dsr
FROM 
(
select *
from
(
select s.*,ROW_NUMBER() OVER ( Partition By shopid order by DateRange desc)  rn
from t_elengjing.shop_new s
) a
where a.rn=1
);