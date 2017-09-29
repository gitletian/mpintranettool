
------字段 意义 ---------------
chongrongliang               充绒量
hanrongliang                 含绒量
tuan                         图案
tuanwenhua                   图案文化
tianchongwu                  填充物
gongyi                       工艺
langxing                     廓形
chengfenhanliang             成分含量
baixing                      摆型
caizhichengfen               材质成分
kuanshi                      款式
banxing                      版型
lifubaixing                  礼服摆型
yaoxing                      腰型
menyijin                     衣门襟
xiuxing                      袖型
xiuchang                     袖长
qunxing                      裙型
qunchang                     裙长
kuxing                       裤型
kuchang                      裤长
jinxing                      襟形
liliao                       里料
mianliao                     面料
lingzi                       领子
fengge                       风格

-----------------------对旧数据进行打标签------------------------------
drop table if exists extract.women_clothing_item_attr_old;
CREATE TABLE if not exists extract.women_clothing_item_attr_old(
 itemid BIGINT
,shopid BIGINT
,categoryid BIGINT
,chongrongliang    String
,hanrongliang    String
,tuan    String
,tuanwenhua    String
,tianchongwu    String
,gongyi    String
,langxing    String
,chengfenhanliang    String
,baixing    String
,caizhichengfen    String
,kuanshi    String
,banxing    String
,lifubaixing    String
,yaoxing    String
,menyijin    String
,xiuxing    String
,xiuchang    String
,qunxing    String
,qunchang    String
,kuxing    String
,kuchang    String
,jinxing    String
,liliao    String
,mianliao    String
,lingzi    String
,fengge    String
,errormessage STRING
)
CLUSTERED BY (ItemID) SORTED BY (ItemID) INTO 113 BUCKETS
STORED AS ORC
;



add jar /home/script/normal_servers/serverudf/elengjing/Tag_18.jar;

drop temporary function if exists Tagged_e_o_18;

create temporary function Tagged_e_o_18 as 'com.marcpoint.elengjing_extend.Tagged_e_o_18';

insert into extract.women_clothing_item_attr_old
select
Tagged_e_o_18(tw.ItemID,tw.categoryid,tw.shopid,tw.itemname,regexp_replace(tw.ItemAttrDesc, '\t', ' '))  as (itemid, shopid, categoryid, chongrongliang, hanrongliang, tuan, tuanwenhua, tianchongwu, gongyi, langxing, chengfenhanliang, baixing, caizhichengfen, kuanshi, banxing, lifubaixing, yaoxing, menyijin, xiuxing, xiuchang, qunxing, qunchang, kuxing, kuchang, jinxing, liliao, mianliao, lingzi, fengge, errormessage)
from
elengjing.women_clothing_item_unique tw
where nvl(tw.itemattrdesc, '') != ''
;



-----------------------对新数据进行打标签------------------------------

drop table if exists elengjing.women_clothing_item_attr_new;
CREATE TABLE if not exists elengjing.women_clothing_item_attr_new(
 itemid BIGINT
,shopid BIGINT
,categoryid BIGINT
,chongrongliang    String
,hanrongliang    String
,tuan    String
,tuanwenhua    String
,tianchongwu    String
,gongyi    String
,langxing    String
,chengfenhanliang    String
,baixing    String
,caizhichengfen    String
,kuanshi    String
,banxing    String
,lifubaixing    String
,yaoxing    String
,menyijin    String
,xiuxing    String
,xiuchang    String
,qunxing    String
,qunchang    String
,kuxing    String
,kuchang    String
,jinxing    String
,liliao    String
,mianliao    String
,lingzi    String
,fengge    String
,errormessage STRING
)
CLUSTERED BY (ItemID) SORTED BY (ItemID) INTO 113 BUCKETS
STORED AS ORC
;



add jar /home/script/normal_servers/serverudf/elengjing/Tag_18.jar;

drop temporary function if exists Tagged_e_n_18;

create temporary function Tagged_e_n_18 as 'com.marcpoint.elengjing_extend.Tagged_e_n_18';

insert into elengjing.women_clothing_item_attr_new
select
Tagged_e_n_18(tw.ItemID,tw.categoryid,tw.shopid,tw.itemname,regexp_replace(tw.ItemAttrDesc, '\t', ' '))  as (itemid, shopid, categoryid, chongrongliang, hanrongliang, tuan, tuanwenhua, tianchongwu, gongyi, langxing, chengfenhanliang, baixing, caizhichengfen, kuanshi, banxing, lifubaixing, yaoxing, menyijin, xiuxing, xiuchang, qunxing, qunchang, kuxing, kuchang, jinxing, liliao, mianliao, lingzi, fengge, errormessage)
from
new_elengjing.women_clothing_item_unique_desc tw
where nvl(tw.itemattrdesc, '') != ''
;



--------------------------------新标签汇总---------------------------------

drop table if exists elengjing.women_clothing_item_attr;
CREATE TABLE if not exists elengjing.women_clothing_item_attr(
 itemid BIGINT
,shopid BIGINT
,categoryid BIGINT
,chongrongliang    String
,hanrongliang    String
,tuan    String
,tuanwenhua    String
,tianchongwu    String
,gongyi    String
,langxing    String
,chengfenhanliang    String
,baixing    String
,caizhichengfen    String
,kuanshi    String
,banxing    String
,lifubaixing    String
,yaoxing    String
,menyijin    String
,xiuxing    String
,xiuchang    String
,qunxing    String
,qunchang    String
,kuxing    String
,kuchang    String
,jinxing    String
,liliao    String
,mianliao    String
,lingzi    String
,fengge    String
)
CLUSTERED BY (itemid) SORTED BY (itemid) INTO 113 BUCKETS
STORED AS ORC
;


INSERT INTO table elengjing.women_clothing_item_attr
SELECT
itemid
,shopid
,categoryid
,chongrongliang
,hanrongliang
,tuan
,tuanwenhua
,tianchongwu
,gongyi
,langxing
,chengfenhanliang
,baixing
,caizhichengfen
,kuanshi
,banxing
,lifubaixing
,yaoxing
,menyijin
,xiuxing
,xiuchang
,qunxing
,qunchang
,kuxing
,kuchang
,jinxing
,liliao
,mianliao
,lingzi
,fengge
from extract.women_clothing_item_attr_old
where ErrorMessage is null or ErrorMessage=""
;


INSERT INTO table elengjing.women_clothing_item_attr
SELECT
 itemid
,shopid
,categoryid
,chongrongliang
,hanrongliang
,tuan
,tuanwenhua
,tianchongwu
,gongyi
,langxing
,chengfenhanliang
,baixing
,caizhichengfen
,kuanshi
,banxing
,lifubaixing
,yaoxing
,menyijin
,xiuxing
,xiuchang
,qunxing
,qunchang
,kuxing
,kuchang
,jinxing
,liliao
,mianliao
,lingzi
,fengge
from extract.women_clothing_item_attr_new
where errormessage is null or errormessage=""
;




---------------------------导出 到pg-----------------------------


drop table if exists transforms.export_women_clothing_item_attr;
CREATE TABLE if not exists transforms.export_women_clothing_item_attr(
 itemid BIGINT
,shopid BIGINT
,categoryid BIGINT

,chongrongliang    String
,hanrongliang    String
,tuan    String
,tianchongwu    String
,gongyi    String
,baixing    String
,caizhichengfen    String
,kuanshi    String
,banxing    String
,yaoxing    String
,xiuxing    String
,qunxing    String
,qunchang    String
,kuxing    String
,liliao    String
,mianliao    String
,lingzi    String
,fengge    String
)
;


insert into table transforms.export_women_clothing_item_attr
select
 *
from elengjing.women_clothing_item_attr
;


export
--connect
jdbc:postgresql://192.168.110.12:5432/elengjing
--username
elengjing
--password
Marcpoint2016
--call
save_women_clothing_item_attr
--num-mappers
95
--input-fields-terminated-by
'\001'
--input-null-string
"\\N"
--input-null-non-string
"\\N"
--export-dir
hdfs://nameservice1/user/hive/warehouse/transforms.db/export_women_clothing_item_attr/


sqoop --options-file ./itemsqoop.txt

---------------------------------save or updat ------------------------------


CREATE OR REPLACE FUNCTION "public"."save_women_clothing_item_attr"( IN in_itemid int8,IN in_shopid int8,IN in_categoryid int8,IN in_chongrongliang varchar,IN in_hanrongliang varchar,IN in_tuan varchar,IN in_tianchongwu varchar,IN in_gongyi varchar,IN in_baixing varchar,IN in_caizhichengfen varchar,IN in_kuanshi varchar,IN in_banxing varchar,IN in_yaoxing varchar,IN in_xiuxing varchar,IN in_qunxing varchar,IN in_qunchang varchar,IN in_kuxing varchar,IN in_liliao varchar,IN in_mianliao varchar,IN in_lingzi varchar,IN in_fengge varchar) RETURNS "void"
	AS $BODY$BEGIN
    LOOP
        -- first try to update the key
        UPDATE women_clothing_item_attr SET shopid = in_shopid, categoryid = in_categoryid, chongrongliang = in_chongrongliang, hanrongliang = in_hanrongliang, tuan = in_tuan, tianchongwu = in_tianchongwu, gongyi = in_gongyi, baixing = in_baixing, caizhichengfen = in_caizhichengfen, kuanshi = in_kuanshi, banxing = in_banxing, yaoxing = in_yaoxing, xiuxing = in_xiuxing, qunxing = in_qunxing, qunchang = in_qunchang, kuxing = in_kuxing, liliao = in_liliao, mianliao = in_mianliao, lingzi = in_lingzi, fengge = in_fengge WHERE itemid = in_itemid;
		IF found THEN
            RETURN;
        END IF;
        -- not there, so try to insert the key
        -- if someone else inserts the same key concurrently,
        -- we could get a unique-key failure
        BEGIN
            INSERT INTO women_clothing_item_attr(itemid, shopid, categoryid, chongrongliang, hanrongliang, tuan, tianchongwu, gongyi, baixing, caizhichengfen, kuanshi, banxing, yaoxing, xiuxing, qunxing, qunchang, kuxing, liliao, mianliao, lingzi, fengge) VALUES (in_itemid, in_shopid, in_categoryid, in_chongrongliang, in_hanrongliang, in_tuan, in_tianchongwu, in_gongyi, in_baixing, in_caizhichengfen, in_kuanshi, in_banxing, in_yaoxing, in_xiuxing, in_qunxing, in_qunchang, in_kuxing, in_liliao, in_mianliao, in_lingzi, in_fengge);
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




DROP TABLE IF EXISTS "public"."women_clothing_item_attr";
CREATE TABLE "public"."women_clothing_item_attr" (
	"itemid" int8 NOT NULL,
  "shopid" int8,
  "categoryid" int8,

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
ALTER TABLE "public"."women_clothing_item_attr" OWNER TO "elengjing";

-- ----------------------------
--  Primary key structure for table women_clothing_item_attr
-- ----------------------------
ALTER TABLE "public"."women_clothing_item_attr" ADD PRIMARY KEY ("itemid") NOT DEFERRABLE INITIALLY IMMEDIATE;






CREATE INDEX chongrongliang ON women_clothing_item_attr (chongrongliang) where chongrongliang is not null;
CREATE INDEX hanrongliang ON women_clothing_item_attr (hanrongliang) where hanrongliang is not null;
CREATE INDEX tuan  ON women_clothing_item_attr (tuan) where tuan  is not null;
CREATE INDEX tianchongwu ON women_clothing_item_attr (tianchongwu) where tianchongwu is not null;
CREATE INDEX gongyi  ON women_clothing_item_attr (gongyi) where gongyi  is not null;
CREATE INDEX baixing ON women_clothing_item_attr (baixing) where baixing is not null;
CREATE INDEX caizhichengfen ON women_clothing_item_attr (caizhichengfen) where caizhichengfen is not null;
CREATE INDEX kuanshi ON women_clothing_item_attr (kuanshi) where kuanshi is not null;
CREATE INDEX banxing ON women_clothing_item_attr (banxing) where banxing is not null;
CREATE INDEX yaoxing ON women_clothing_item_attr (yaoxing) where yaoxing is not null;
CREATE INDEX xiuxing ON women_clothing_item_attr (xiuxing) where xiuxing is not null;
CREATE INDEX qunxing ON women_clothing_item_attr (qunxing) where qunxing is not null;
CREATE INDEX qunchang  ON women_clothing_item_attr (qunchang) where qunchang  is not null;
CREATE INDEX kuxing  ON women_clothing_item_attr (kuxing) where kuxing  is not null;
CREATE INDEX liliao  ON women_clothing_item_attr (liliao) where liliao  is not null;
CREATE INDEX mianliao  ON women_clothing_item_attr (mianliao) where mianliao  is not null;
CREATE INDEX lingzi  ON women_clothing_item_attr (lingzi) where lingzi  is not null;
CREATE INDEX fengge  ON women_clothing_item_attr (fengge) where fengge  is not null;