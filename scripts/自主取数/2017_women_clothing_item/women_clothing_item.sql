----------------------------------women_clothing_item 的表结构 设计----------------------------------------------------
----------
--------------------------------------------------------------------------------------

--------------------------------------- 增量数据处理 --------------------------------------
--------1、 计算当天数据的 环比 和 同比增长率 以及 库变

--------------------------------------- 月数据导入到pg --------------------------------------




drop table if exists transforms.export_women_clothing_item;
create table  if not exists transforms.export_women_clothing_item(
 itemID                BIGINT
,itemname              string
,dateRange             date
,salesqty              BIGINT
,salesamt              decimal(20,2)
,discountPrice         decimal(20,2)
,similarPrice          BIGINT
,shopID                BIGINT
,shopName              string
,categoryID            BIGINT
,categoryName          string
,platformID            int
,listedDate            string
,favorites             BIGINT

,stock                 BIGINT
,discount              BIGINT

,stockAdd              BIGINT
,stockNew              BIGINT

,linkSalesqty          BIGINT
,yearSalesqty          BIGINT
,linkSalesamt          decimal(20,2)
,yearSalesamt          decimal(20,2)

)
;




insert into table transforms.export_women_clothing_item
select
*
from
elengjing.women_clothing_item_new_dict
where daterange > '2016-11-30'
;



export
--connect
jdbc:postgresql://192.168.110.12:5432/elengjing
--username
elengjing
--password
Marcpoint2016
--call
save_women_clothing_item
--num-mappers
95
--input-fields-terminated-by
'\001'
--input-null-string
"\\N"
--input-null-non-string
"\\N"
--export-dir
hdfs://nameservice1/user/hive/warehouse/transforms.db/export_women_clothing_item/


sqoop --options-file ./itemsqoop.txt

---------------------------------save or updat ------------------------------


CREATE OR REPLACE FUNCTION "public"."save_women_clothing_item"(IN in_itemid int8, IN in_itemname varchar, IN in_daterange date, IN in_salesqty int8, IN in_salesamt float8, IN in_discountprice numeric, IN in_similarprice int8, IN in_shopid int8, IN in_shopname varchar, IN in_categoryid int8, IN in_categoryname varchar, IN in_platformid int4, IN in_listeddate timestamp, IN in_favorites int4, IN in_stock int8, IN in_discount int4, IN in_stockadd int8, IN in_stocknew int8, IN in_linksalesqty int8, IN in_yearsalesqty int8, IN in_linksalesamt float8, IN in_yearsalesamt float8) RETURNS "void"
	AS $BODY$BEGIN
    LOOP
        -- first try to update the key
        UPDATE women_clothing_item SET itemname= in_itemname, salesqty = in_salesqty, salesamt = in_salesamt, discountPrice = in_discountPrice, similarPrice = in_similarPrice, shopID = in_shopID, shopName = in_shopName, categoryID = in_categoryID, categoryName = in_categoryName, platformID = in_platformID, listedDate = in_listedDate, favorites = in_favorites, stock = in_stock, discount = in_discount, stockAdd = in_stockAdd, stockNew = in_stockNew, linkSalesqty = in_linkSalesqty, yearSalesqty = in_yearSalesqty, linkSalesamt = in_linkSalesamt, yearSalesamt = in_yearSalesamt WHERE itemid = in_itemid and dateRange = in_dateRange;
        IF found THEN
            RETURN;
        END IF;
        -- not there, so try to insert the key
        -- if someone else inserts the same key concurrently,
        -- we could get a unique-key failure
        BEGIN
            INSERT INTO women_clothing_item(itemID, itemname, dateRange, salesqty, salesamt, discountPrice, similarPrice, shopID, shopName, categoryID, categoryName, platformID, listedDate, favorites, stock, discount, stockAdd, stockNew, linkSalesqty, yearSalesqty, linkSalesamt, yearSalesamt) VALUES (in_itemID, in_itemname, in_dateRange, in_salesqty, in_salesamt, in_discountPrice, in_similarPrice, in_shopID, in_shopName, in_categoryID, in_categoryName, in_platformID, in_listedDate, in_favorites, in_stock, in_discount, in_stockAdd, in_stockNew, in_linkSalesqty, in_yearSalesqty, in_linkSalesamt, in_yearSalesamt);
            RETURN;

        EXCEPTION WHEN unique_violation THEN
            -- Do nothing, and loop to try the UPDATE again.
        END;
    END LOOP;
END;$BODY$
	LANGUAGE plpgsql
	COST 100
	CALLED ON NULL INPUT
	SECURITY INVOKER
	VOLATILE;


---------------------------------pg table ------------------------------


-- ----------------------------
DROP TABLE IF EXISTS "public"."women_clothing_item";
CREATE TABLE "public"."women_clothing_item" (
	"itemid" int8 NOT NULL,
	"itemname" varchar COLLATE "default",
	"daterange" date,
	"salesqty" int8,
	"salesamt" float8,
	"discountprice" numeric,
	"similarprice" int8,
	"shopid" int8,
	"shopname" varchar COLLATE "default",
	"categoryid" int8,
	"categoryname" varchar COLLATE "default",
	"platformid" int4,
	"listeddate" timestamp(6) NULL,
	"favorites" int4,
	"stock" int8,
	"discount" int4,
	"stockadd" int8,
	"stocknew" int8,
	"linksalesqty" int8,
	"yearsalesqty" int8,
	"linksalesamt" float8,
	"yearsalesamt" float8
)
WITH (OIDS=FALSE);
ALTER TABLE "public"."women_clothing_item" OWNER TO "elengjing";

-- ----------------------------
--  Primary key structure for table women_clothing_item
-- ----------------------------
ALTER TABLE "public"."women_clothing_item" ADD PRIMARY KEY ("itemid", "daterange") NOT DEFERRABLE INITIALLY IMMEDIATE;


CREATE INDEX shopid ON women_clothing_item (shopid);






--------------------------------创建数据view-----------------------------
drop view if EXISTS elengjing.women_clothing_item_v5;
CREATE VIEW IF NOT EXISTS elengjing.women_clothing_item_v5
AS
select
 t1.itemid
,t1.daterange
,cast(t3.year as string) as years
,t3.yearquartal
,t3.yearmonth
,t3.weekperiodmmdd
,t1.salesqty
,t1.salesamt
,t1.similarprice
,t1.shopid
,t1.discountPrice
,t1.shopname
,t1.categoryid
,t1.categoryname
,t1.platformid
,t1.discount

,t1.stockAdd
,t1.stockNew

,t2.tuan
,t2.mianliao
,t2.banxing
,t2.fengge
,t2.gongyi
,t2.kuanshi
,t2.qunxing
,t2.yaoxing
,t2.kuxing
,t2.xiuxing
,t2.baixing
,t2.lingzi
,t2.qunchang
,t2.liliao
,t2.caizhichengfen
,t2.tianchongwu
,t2.chongrongliang
,t2.hanrongliang

from
elengjing.WOMEN_CLOTHING_ITEM_NEW_DICT t1
left join
elengjing.WOMEN_CLOTHING_ITEM_ATTR t2
on t1.itemid = t2.itemid
left JOIN
elengjing.dimdate t3
on t1.daterange = t3.daterange
;
