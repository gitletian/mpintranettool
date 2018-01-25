drop table if exists extract.milk_data;
CREATE TABLE if not exists extract.milk_data(
 daterange      string
,categoryname   string
,itemid         string
,itemname       string
,itemurl        string
,mainpicurl     string
,itemattrdesc   string
,listprice      string
,discountprice  string
,salesqty       string
,salesamt       string
,totalorders    string
,platfrom       string
,sku            string
,skuinfo        string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\001'
;


LOAD DATA LOCAL INPATH '/home/tmp/20171225.csv'  INTO TABLE extract.milk_data
;



drop table if exists transforms.milk_data;
CREATE TABLE  if not exists transforms.milk_data(
 daterange      string
,categoryname   string
,itemid         string
,itemname       string
,itemurl        string
,mainpicurl     string
,itemattrdesc   string
,listprice      string
,discountprice  string
,salesqty       string
,salesamt       string
,totalorders    string
,platfrom       string
,sku            string
,skuinfo        string
)
CLUSTERED BY (itemid) INTO 97 BUCKETS
STORED AS ORC
;


INSERT into transforms.milk_data
select * from extract.milk_data
;






drop table if exists transforms.milk_data_end;
CREATE TABLE  if not exists transforms.milk_data_end(
 daterange      string
,categoryname   string
,itemid         string
,itemname       string
,itemurl        string
,mainpicurl     string
,itemattrdesc   string
,listprice      string
,discountprice  string
,salesqty       string
,salesamt       string
,totalorders    string
,platfrom       string
,sku            string
,skuinfo        string
,error_info     string
)
STORED AS ORC
;

add file /home/tmp/script/milk_data_05.py;


set daterange=201609;
set daterange=201610;
set daterange=201611;
set daterange=201612;
set daterange=201701;
set daterange=201702;
set daterange=201703;
set daterange=201704;
set daterange=201705;
set daterange=201706;
set daterange=201707;
set daterange=201708;
set daterange=201709;
set daterange=201710;
set daterange=201711;




with last_data as (
    select
     daterange
    ,categoryname
    ,itemid
    ,itemname
    ,itemurl
    ,mainpicurl
    ,itemattrdesc
    ,listprice
    ,discountprice
    ,salesqty
    ,salesamt
    ,totalorders
    ,platfrom
    ,sku
    ,skuinfo
    from
    (
        select
        *,
        ROW_NUMBER() OVER (Partition by itemid ORDER BY daterange desc) AS rn
        from
        transforms.milk_data
        where daterange < ${hiveconf:daterange}
    ) t
    where rn = 1
),
today_day as (
    select
    *
    from
    transforms.milk_data
    where daterange = ${hiveconf:daterange}
)

INSERT into transforms.milk_data_end
select
TRANSFORM (t1.categoryname, t1.itemid, t1.itemname, t1.itemurl, t1.mainpicurl, t1.itemattrdesc, t1.listprice, t1.discountprice, t1.salesqty, t1.salesamt, t1.totalorders, t1.platfrom, t1.sku, t1.skuinfo, t2.sku, t1.daterange)
USING "python milk_data_05.py"
AS (daterange, categoryname, itemid, itemname, itemurl, mainpicurl, itemattrdesc, listprice, discountprice, salesqty, salesamt, totalorders, platfrom, sku, skuinfo, error_info)
from
today_day t1
left join
last_data t2
on t1.itemid = t2.itemid
;




insert overwrite local directory '/home/tmp/milk_data/'
row format delimited fields terminated by '\001'

select
 daterange
,categoryname
,itemid
,itemname
,itemurl
,mainpicurl
,itemattrdesc
,listprice
,discountprice
,salesqty
,salesamt
,totalorders
,platfrom
,sku
,skuinfo
from
transforms.milk_data_end
;





drop table if exists eb.medela_mjw;
CREATE TABLE if not exists eb.medela_mjw(
 daterange         string
,categoryname      string
,itemid            string
,itemname          string
,itemurl           string
,mainpicurl        string
,itemattrdesc      string
,listprice         string
,discountprice     string
,salesqty          string
,salesamt          string
,totalorders       string
,platfrom          string
,cate              string
,caliber           string
,brand             string
,material          string
,handle            string
,volume            string
,package           string
,hole              string
,flow              string
,norm              string
,drops             string
,skuid             string
,skulistprice      string
,skudiscountprice  string
,skustock          string
,skusalesqty       string
,skusalesamt       string
,skusalesqtynew    string
,skusalesamtnew    string
,skuname           string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\001'
;


LOAD DATA LOCAL INPATH '/home/tmp/milk_data_result.csv'  INTO TABLE eb.medela_mjw
;
