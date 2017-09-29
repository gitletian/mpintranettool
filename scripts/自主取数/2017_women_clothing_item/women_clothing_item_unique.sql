----------------------------------  women_clothing_item_unique 的表结构 设计----------------------------------------------------
----------
--------------------------------------------------------------------------------------

--------------------------------------- 总 数据导入到pg --------------------------------------
 ----1、 创建txt表



drop table if exists transforms.export_women_clothing_item_unique_desc;
CREATE table  if not exists transforms.export_women_clothing_item_unique_desc(
 itemid                bigint
,itemname              string
,daterange             date
,itemurl               string
,itemsubtitle          string
,mainpicurl            string
,listprice             decimal(20,2)
,discountprice         bigint
,salesqty              bigint
,salesamt              decimal(20,2)
,favorites             bigint
,stock                 bigint
,categoryid            bigint
,categoryname          string
,shopid                bigint
,shopname              string
,shopurl               string
,listeddate            string
,platformid            int

,towWeekSalesqty       bigint
,monthSalesqty         bigint
,shopsalesqty          bigint
,linkSalesqty          bigint
,yearSalesqty          bigint

,towWeekSalesamt       decimal(20,2)
,monthSalesamt         decimal(20,2)
,linkSalesamt          decimal(20,2)
,yearSalesamt          decimal(20,2)

,dailyPrice            decimal(20, 2)
,eventPrice            decimal(20, 2)
,dailyPriceRatio       decimal(20, 2)
,eventPriceRatio       decimal(20, 2)

)
;


insert into table transforms.export_women_clothing_item_unique_desc
select
 *
from elengjing.women_clothing_item_unique_new_dict
;


----2、sqoop数据导入

export
--connect
jdbc:postgresql://192.168.110.12:5432/elengjing
--username
elengjing
--password
Marcpoint2016
--call
save_women_clothing_item_unique
--num-mappers
95
--input-fields-terminated-by
'\001'
--input-null-string
"\\N"
--input-null-non-string
"\\N"
--export-dir
hdfs://nameservice1/user/hive/warehouse/transforms.db/export_women_clothing_item_unique_desc/


sqoop --options-file ./itemsqoop.txt

---------------------------------save or updat ------------------------------


CREATE OR REPLACE FUNCTION "public"."save_women_clothing_item_unique"(IN in_itemid int8, IN in_itemname varchar, IN in_daterange date, IN in_itemurl varchar, IN in_itemsubtitle varchar, IN in_mainpicurl varchar, IN in_listprice float8, IN in_discountprice int8, IN in_salesqty int8, IN in_salesamt float8, IN in_favorites int8, IN in_stock int8, IN in_categoryid int8, IN in_categoryname varchar, IN in_shopid int8, IN in_shopname varchar, IN in_shopurl varchar, IN in_listeddate timestamp, IN in_platformid int4, IN in_towweeksalesqty int8, IN in_monthsalesqty int8, IN in_shopsalesqty int8, IN in_linksalesqty int8, IN in_yearsalesqty int8, IN in_towweeksalesamt float8, IN in_monthsalesamt float8, IN in_linksalesamt float8, IN in_yearsalesamt float8, IN in_dailyprice float8, IN in_eventprice float8, IN in_dailypriceratio float8, IN in_eventpriceratio float8) RETURNS "void"
	AS $BODY$BEGIN
    LOOP
        -- first try to update the key
        UPDATE women_clothing_item_unique SET itemname = in_itemname, daterange = in_daterange, itemurl = in_itemurl, itemsubtitle = in_itemsubtitle, mainpicurl = in_mainpicurl, listprice = in_listprice, discountprice = in_discountprice, salesqty = in_salesqty, salesamt = in_salesamt, favorites = in_favorites, stock = in_stock, categoryid = in_categoryid, categoryname = in_categoryname, shopid = in_shopid, shopname = in_shopname, shopurl = in_shopurl, listeddate = in_listeddate, platformid = in_platformid, towweeksalesqty = in_towweeksalesqty, monthsalesqty = in_monthsalesqty, shopsalesqty = in_shopsalesqty, linksalesqty = in_linksalesqty, yearsalesqty = in_yearsalesqty, towweeksalesamt = in_towweeksalesamt, monthsalesamt = in_monthsalesamt, linksalesamt = in_linksalesamt, yearsalesamt = in_yearsalesamt, dailyprice = in_dailyprice, eventprice = in_eventprice, dailypriceratio = in_dailypriceratio, eventpriceratio = in_eventpriceratio WHERE itemid = in_itemid;
		IF found THEN
            RETURN;
        END IF;
        -- not there, so try to insert the key
        -- if someone else inserts the same key concurrently,
        -- we could get a unique-key failure
        BEGIN
            INSERT INTO women_clothing_item_unique(itemid, itemname, daterange, itemurl, itemsubtitle, mainpicurl, listprice, discountprice, salesqty, salesamt, favorites, stock, categoryid, categoryname, shopid, shopname, shopurl, listeddate, platformid, towweeksalesqty, monthsalesqty, shopsalesqty, linksalesqty, yearsalesqty, towweeksalesamt, monthsalesamt, linksalesamt, yearsalesamt, dailyprice, eventprice, dailypriceratio, eventpriceratio) VALUES (in_itemid, in_itemname, in_daterange, in_itemurl, in_itemsubtitle, in_mainpicurl, in_listprice, in_discountprice, in_salesqty, in_salesamt, in_favorites, in_stock, in_categoryid, in_categoryname, in_shopid, in_shopname, in_shopurl, in_listeddate, in_platformid, in_towweeksalesqty, in_monthsalesqty, in_shopsalesqty, in_linksalesqty, in_yearsalesqty, in_towweeksalesamt, in_monthsalesamt, in_linksalesamt, in_yearsalesamt, in_dailyprice, in_eventprice, in_dailypriceratio, in_eventpriceratio);
            RETURN;

        EXCEPTION WHEN unique_violation THEN
            -- Do nothing, and loop to try the UPDATE again.
        END;
    END LOOP;
END;
$BODY$
	LANGUAGE plpgsql
	COST 100
	CALLED ON NULL INPUT
	SECURITY INVOKER
	VOLATILE;


---------------------------------pg table  ------------------------------

-- ----------------------------
--  Table structure for women_clothing_item_unique_desc
-- ----------------------------
DROP TABLE IF EXISTS "public"."women_clothing_item_unique";
CREATE TABLE "public"."women_clothing_item_unique" (
	"itemid" int8 NOT NULL,
	"itemname" varchar COLLATE "default",
	"daterange" date,
	"itemurl" varchar COLLATE "default",
	"itemsubtitle" varchar COLLATE "default",
	"mainpicurl" varchar COLLATE "default",
	"listprice" float8,
	"discountprice" int8,
	"salesqty" int8,
	"salesamt" float8,
	"favorites" int8,
	"stock" int8,
	"categoryid" int8,
	"categoryname" varchar COLLATE "default",
	"shopid" int8,
	"shopname" varchar COLLATE "default",
	"shopurl" varchar COLLATE "default",
	"listeddate" timestamp(6) NULL,
	"platformid" int4,
	"towweeksalesqty" int8,
	"monthsalesqty" int8,
	"shopsalesqty" int8,
	"linksalesqty" int8,
	"yearsalesqty" int8,
	"towweeksalesamt" float8,
	"monthsalesamt" float8,
	"linksalesamt" float8,
	"yearsalesamt" float8,
	"dailyprice" float8,
	"eventprice" float8,
	"dailypriceratio" float8,
	"eventpriceratio" float8
)
WITH (OIDS=FALSE);
ALTER TABLE "public"."women_clothing_item_unique" OWNER TO "elengjing";

-- ----------------------------
--  Primary key structure for table women_clothing_item_unique
-- ----------------------------
ALTER TABLE "public"."women_clothing_item_unique" ADD PRIMARY KEY ("itemid") NOT DEFERRABLE INITIALLY IMMEDIATE;


CREATE INDEX shopid ON women_clothing_item_unique (shopid);








------------------------------------------------------------------unique attr  ------------------------------------------------------------



--------------------------------------- 总 数据导入到pg --------------------------------------
 ----1、 创建txt表



drop table if exists transforms.export_women_clothing_item_unique_attr;
CREATE table  if not exists transforms.export_women_clothing_item_unique_attr(
 itemid                bigint
,itemname              string
,daterange             date
,itemurl               string
,itemsubtitle          string
,mainpicurl            string
,listprice             decimal(20,2)
,discountprice         bigint
,salesqty              bigint
,salesamt              decimal(20,2)
,favorites             bigint
,stock                 bigint
,categoryid            bigint
,categoryname          string
,shopid                bigint
,shopname              string
,shopurl               string
,listeddate            string
,platformid            int

,towWeekSalesqty       bigint
,monthSalesqty         bigint
,shopsalesqty          bigint
,linkSalesqty          bigint
,yearSalesqty          bigint


,towWeekSalesamt       decimal(20,2)
,monthSalesamt         decimal(20,2)
,linkSalesamt          decimal(20,2)
,yearSalesamt          decimal(20,2)

,dailyPrice            decimal(20, 2)
,eventPrice            decimal(20, 2)
,dailyPriceRatio       decimal(20, 2)
,eventPriceRatio       decimal(20, 2)


,chongrongliang        String
,hanrongliang          String
,tuan                  String
,tianchongwu           String
,gongyi                String
,baixing               String
,caizhichengfen        String
,kuanshi               String
,banxing               String
,yaoxing               String
,xiuxing               String
,qunxing               String
,qunchang              String
,kuxing                String
,liliao                String
,mianliao              String
,lingzi                String
,fengge                String
)
;


insert into table transforms.export_women_clothing_item_unique_attr
select

 t1.itemid
,t1.itemname
,t1.daterange
,t1.itemurl
,t1.itemsubtitle
,t1.mainpicurl
,t1.listprice
,t1.discountprice
,t1.salesqty
,t1.salesamt
,t1.favorites
,t1.stock
,t1.categoryid
,t1.categoryname
,t1.shopid
,t1.shopname
,t1.shopurl
,t1.listeddate
,t1.platformid
,t1.towWeekSalesqty
,t1.monthSalesqty
,t1.shopsalesqty
,t1.linkSalesqty
,t1.yearSalesqty

,t1.towWeekSalesamt
,t1.monthSalesamt
,t1.linkSalesamt
,t1.yearSalesamt
,t1.dailyPrice
,t1.eventPrice
,t1.dailyPriceRatio
,t1.eventPriceRatio


,t2.chongrongliang
,t2.hanrongliang
,t2.tuan
,t2.tianchongwu
,t2.gongyi
,t2.baixing
,t2.caizhichengfen
,t2.kuanshi
,t2.banxing
,t2.yaoxing
,t2.xiuxing
,t2.qunxing
,t2.qunchang
,t2.kuxing
,t2.liliao
,t2.mianliao
,t2.lingzi
,t2.fengge

from
elengjing.women_clothing_item_unique_new_dict t1
left JOIN
elengjing.women_clothing_item_attr t2
on t1.itemid = t2.itemid
where t1.daterange > '2016-12-01'
;


----2、sqoop数据导入

export
--connect
jdbc:postgresql://192.168.110.12:5432/elengjing
--username
elengjing
--password
Marcpoint2016
--call
save_women_clothing_item_unique_attr
--num-mappers
85
--input-fields-terminated-by
'\001'
--input-null-string
"\\N"
--input-null-non-string
"\\N"
--export-dir
hdfs://nameservice1/user/hive/warehouse/transforms.db/export_women_clothing_item_unique_attr/


sqoop --options-file ./itemsqoop.txt

---------------------------------save or updat ------------------------------


CREATE OR REPLACE FUNCTION "public"."save_women_clothing_item_unique_attr"(IN in_itemid int8, IN in_itemname varchar, IN in_daterange date, IN in_itemurl varchar, IN in_itemsubtitle varchar, IN in_mainpicurl varchar, IN in_listprice float8, IN in_discountprice int8, IN in_salesqty int8, IN in_salesamt float8, IN in_favorites int8, IN in_stock int8, IN in_categoryid int8, IN in_categoryname varchar, IN in_shopid int8, IN in_shopname varchar, IN in_shopurl varchar, IN in_listeddate timestamp, IN in_platformid int4, IN in_towweeksalesqty int8, IN in_monthsalesqty int8, IN in_shopsalesqty int8, IN in_linksalesqty int8, IN in_yearsalesqty int8, IN in_towweeksalesamt float8, IN in_monthsalesamt float8, IN in_linksalesamt float8, IN in_yearsalesamt float8, IN in_dailyprice float8, IN in_eventprice float8, IN in_dailypriceratio float8, IN in_eventpriceratio float8, IN in_chongrongliang varchar,IN in_hanrongliang varchar,IN in_tuan varchar,IN in_tianchongwu varchar,IN in_gongyi varchar,IN in_baixing varchar,IN in_caizhichengfen varchar,IN in_kuanshi varchar,IN in_banxing varchar,IN in_yaoxing varchar,IN in_xiuxing varchar,IN in_qunxing varchar,IN in_qunchang varchar,IN in_kuxing varchar,IN in_liliao varchar,IN in_mianliao varchar,IN in_lingzi varchar,IN in_fengge varchar) RETURNS "void"
  AS $BODY$BEGIN
    LOOP
        -- first try to update the key
        UPDATE women_clothing_item_unique_attr SET itemname = in_itemname, daterange = in_daterange, itemurl = in_itemurl, itemsubtitle = in_itemsubtitle, mainpicurl = in_mainpicurl, listprice = in_listprice, discountprice = in_discountprice, salesqty = in_salesqty, salesamt = in_salesamt, favorites = in_favorites, stock = in_stock, categoryid = in_categoryid, categoryname = in_categoryname, shopid = in_shopid, shopname = in_shopname, shopurl = in_shopurl, listeddate = in_listeddate, platformid = in_platformid, towweeksalesqty = in_towweeksalesqty, monthsalesqty = in_monthsalesqty, shopsalesqty = in_shopsalesqty, linksalesqty = in_linksalesqty, yearsalesqty = in_yearsalesqty, towweeksalesamt = in_towweeksalesamt, monthsalesamt = in_monthsalesamt, linksalesamt = in_linksalesamt, yearsalesamt = in_yearsalesamt, dailyprice = in_dailyprice, eventprice = in_eventprice, dailypriceratio = in_dailypriceratio, eventpriceratio = in_eventpriceratio , chongrongliang = in_chongrongliang, hanrongliang = in_hanrongliang, tuan = in_tuan, tianchongwu = in_tianchongwu, gongyi = in_gongyi, baixing = in_baixing, caizhichengfen = in_caizhichengfen, kuanshi = in_kuanshi, banxing = in_banxing, yaoxing = in_yaoxing, xiuxing = in_xiuxing, qunxing = in_qunxing, qunchang = in_qunchang, kuxing = in_kuxing, liliao = in_liliao, mianliao = in_mianliao, lingzi = in_lingzi, fengge = in_fengge WHERE itemid = in_itemid;
    IF found THEN
            RETURN;
        END IF;
        -- not there, so try to insert the key
        -- if someone else inserts the same key concurrently,
        -- we could get a unique-key failure
        BEGIN
            INSERT INTO women_clothing_item_unique_attr(itemid, itemname, daterange, itemurl, itemsubtitle, mainpicurl, listprice, discountprice, salesqty, salesamt, favorites, stock, categoryid, categoryname, shopid, shopname, shopurl, listeddate, platformid, towweeksalesqty, monthsalesqty, shopsalesqty, linksalesqty, yearsalesqty, towweeksalesamt, monthsalesamt, linksalesamt, yearsalesamt, dailyprice, eventprice, dailypriceratio, eventpriceratio, chongrongliang, hanrongliang, tuan, tianchongwu, gongyi, baixing, caizhichengfen, kuanshi, banxing, yaoxing, xiuxing, qunxing, qunchang, kuxing, liliao, mianliao, lingzi, fengge) VALUES (in_itemid, in_itemname, in_daterange, in_itemurl, in_itemsubtitle, in_mainpicurl, in_listprice, in_discountprice, in_salesqty, in_salesamt, in_favorites, in_stock, in_categoryid, in_categoryname, in_shopid, in_shopname, in_shopurl, in_listeddate, in_platformid, in_towweeksalesqty, in_monthsalesqty, in_shopsalesqty, in_linksalesqty, in_yearsalesqty, in_towweeksalesamt, in_monthsalesamt, in_linksalesamt, in_yearsalesamt, in_dailyprice, in_eventprice, in_dailypriceratio, in_eventpriceratio, in_chongrongliang, in_hanrongliang, in_tuan, in_tianchongwu, in_gongyi, in_baixing, in_caizhichengfen, in_kuanshi, in_banxing, in_yaoxing, in_xiuxing, in_qunxing, in_qunchang, in_kuxing, in_liliao, in_mianliao, in_lingzi, in_fengge);
            RETURN;

        EXCEPTION WHEN unique_violation THEN
            -- Do nothing, and loop to try the UPDATE again.
        END;
    END LOOP;
END;
$BODY$
  LANGUAGE plpgsql
  COST 100
  CALLED ON NULL INPUT
  SECURITY INVOKER
  VOLATILE;

---------------------------------pg table  ------------------------------

-- ----------------------------
--  Table structure for women_clothing_item_unique_desc
-- ----------------------------
DROP TABLE IF EXISTS "public"."women_clothing_item_unique_attr";
CREATE TABLE "public"."women_clothing_item_unique_attr" (
	"itemid" int8 NOT NULL,
	"itemname" varchar COLLATE "default",
	"daterange" date,
	"itemurl" varchar COLLATE "default",
	"itemsubtitle" varchar COLLATE "default",
	"mainpicurl" varchar COLLATE "default",
	"listprice" float8,
	"discountprice" int8,
	"salesqty" int8,
	"salesamt" float8,
	"favorites" int8,
	"stock" int8,
	"categoryid" int8,
	"categoryname" varchar COLLATE "default",
	"shopid" int8,
	"shopname" varchar COLLATE "default",
	"shopurl" varchar COLLATE "default",
	"listeddate" timestamp(6) NULL,
	"platformid" int4,
	"towweeksalesqty" int8,
	"monthsalesqty" int8,
	"shopsalesqty" int8,
	"linksalesqty" int8,
	"yearsalesqty" int8,
	"towweeksalesamt" float8,
	"monthsalesamt" float8,
	"linksalesamt" float8,
	"yearsalesamt" float8,
	"dailyprice" float8,
	"eventprice" float8,
	"dailypriceratio" float8,
	"eventpriceratio" float8,

	"chongrongliang" varchar COLLATE "default",
  "hanrongliang" varchar COLLATE "default",
  "tuan" varchar COLLATE "default",
  "tianchongwu" varchar COLLATE "default",
  "gongyi" varchar COLLATE "default",
  "baixing" varchar COLLATE "default",
  "caizhichengfen" varchar COLLATE "default",
  "kuanshi" varchar COLLATE "default",
  "banxing" varchar COLLATE "default",
  "yaoxing" varchar COLLATE "default",
  "xiuxing" varchar COLLATE "default",
  "qunxing" varchar COLLATE "default",
  "qunchang" varchar COLLATE "default",
  "kuxing" varchar COLLATE "default",
  "liliao" varchar COLLATE "default",
  "mianliao" varchar COLLATE "default",
  "lingzi" varchar COLLATE "default",
  "fengge" varchar COLLATE "default"
)
WITH (OIDS=FALSE);
ALTER TABLE "public"."women_clothing_item_unique_attr" OWNER TO "elengjing";

-- ----------------------------
--  Primary key structure for table women_clothing_item_unique
-- ----------------------------
ALTER TABLE "public"."women_clothing_item_unique_attr" ADD PRIMARY KEY ("itemid") NOT DEFERRABLE INITIALLY IMMEDIATE;


CREATE INDEX shopid ON women_clothing_item_unique_attr (shopid);
CREATE INDEX discountprice ON women_clothing_item_unique_attr (discountprice);
CREATE INDEX categoryid ON women_clothing_item_unique_attr (categoryid);
CREATE INDEX daterange  ON women_clothing_item_unique_attr (daterange);


CREATE INDEX chongrongliang ON women_clothing_item_unique_attr (chongrongliang) where chongrongliang is not null;
CREATE INDEX hanrongliang ON women_clothing_item_unique_attr (hanrongliang) where hanrongliang is not null;
CREATE INDEX tuan  ON women_clothing_item_unique_attr (tuan) where tuan  is not null;
CREATE INDEX tianchongwu ON women_clothing_item_unique_attr (tianchongwu) where tianchongwu is not null;
CREATE INDEX gongyi  ON women_clothing_item_unique_attr (gongyi) where gongyi  is not null;
CREATE INDEX baixing ON women_clothing_item_unique_attr (baixing) where baixing is not null;
CREATE INDEX caizhichengfen ON women_clothing_item_unique_attr (caizhichengfen) where caizhichengfen is not null;
CREATE INDEX kuanshi ON women_clothing_item_unique_attr (kuanshi) where kuanshi is not null;
CREATE INDEX banxing ON women_clothing_item_unique_attr (banxing) where banxing is not null;
CREATE INDEX yaoxing ON women_clothing_item_unique_attr (yaoxing) where yaoxing is not null;
CREATE INDEX xiuxing ON women_clothing_item_unique_attr (xiuxing) where xiuxing is not null;
CREATE INDEX qunxing ON women_clothing_item_unique_attr (qunxing) where qunxing is not null;
CREATE INDEX qunchang  ON women_clothing_item_unique_attr (qunchang) where qunchang  is not null;
CREATE INDEX kuxing  ON women_clothing_item_unique_attr (kuxing) where kuxing  is not null;
CREATE INDEX liliao  ON women_clothing_item_unique_attr (liliao) where liliao  is not null;
CREATE INDEX mianliao  ON women_clothing_item_unique_attr (mianliao) where mianliao  is not null;
CREATE INDEX lingzi  ON women_clothing_item_unique_attr (lingzi) where lingzi  is not null;
CREATE INDEX fengge  ON women_clothing_item_unique_attr (fengge) where fengge  is not null;





