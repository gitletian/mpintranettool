--------------------------------------------------------------------------------------
----------                       weibo 数据迁移及定时入库
---------- 1、微博原始数据迁移
---------- 2、微博旧字典增量入库
---------- 3、微博新字典增量入库
----------
--------------------------------------------------------------------------------------

---- 1、 微博原始数据迁移

set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
-- 入库
drop table if exists extract.socialmedia_weibo_txt;
CREATE TABLE  if not exists extract.socialmedia_weibo_txt(
id                  string   ,
created_at          string   ,
text                string   ,
source              string   ,
geo                 string   ,
pic_urls            string   ,
uid                 string   ,
province            string   ,
city                string   ,
ulocation           string   ,
ugender             string   ,
ufollowers_count    string   ,
ufriends_count      string   ,
ustatuses_count     string   ,
ufavourites_count   string   ,
uverified           string   ,
verified_reason     string   ,
ubi_followers_count string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY'\t';

LOAD DATA LOCAL INPATH '/mnt/disk1/weibo/*/*.txt'  INTO TABLE extract.socialmedia_weibo_txt;
LOAD DATA LOCAL INPATH '/mnt/disk2/weibo/*/*.txt'  INTO TABLE extract.socialmedia_weibo_txt;
LOAD DATA LOCAL INPATH '/mnt/disk3/weibo/*/*.txt'  INTO TABLE extract.socialmedia_weibo_txt;


-- 预处理
drop table if exists transforms.socialmedia_weibo;
CREATE TABLE  if not exists transforms.socialmedia_weibo(
id  string,
platform_id  int,
post_id  string,
user_id  string,
user_name  string,
user_location  string,
transmit_post_id  string,
content  string,
content_type  int,
image_urls  string,
device  string,
verified_type  string,
comment_count  int,
like_count  int,
transmit_count  int,
detail_url  string,
updated_at  string,
created_at  string,
noise  int
)
PARTITIONED BY (daterange string)
STORED AS orc
;



insert into transforms.socialmedia_weibo PARTITION(daterange)
select
reflect("java.util.UUID", "randomUUID"),
3002                    ,
id                      ,
if(nvl(uid, '') != '', concat('3002', ":", uid), uid),
''                      ,
ulocation               ,
id                      ,
text                    ,
0                       ,
pic_urls                ,
regexp_extract(source,'>(.*)<',1),
verified_reason         ,
''                      ,
''                      ,
''                      ,
''                      ,
from_unixtime(unix_timestamp(created_at,"EEE MMM dd HH:mm:ss z yyyy"),"yyyy-MM-dd HH:mm:ss"),
from_unixtime(unix_timestamp(created_at,"EEE MMM dd HH:mm:ss z yyyy"),"yyyy-MM-dd HH:mm:ss"),
'',
from_unixtime(unix_timestamp(created_at,"EEE MMM dd HH:mm:ss z yyyy"),"yyyyMM")
from extract.socialmedia_weibo_txt
where nvl(created_at, '') != ''
;


drop table if exists transforms.socialmedia_weibo_error;
CREATE TABLE  if not exists transforms.socialmedia_weibo_error
STORED AS orc
as
select
*
from extract.socialmedia_weibo_txt
where  nvl(created_at, '') = ''
;

-- 去水










---- 2、 微博旧字典增量入库

-- 入库
drop table if exists extract.socialmedia_weibo_add_txt;
CREATE TABLE  if not exists extract.socialmedia_weibo_add_txt(
id                  string   ,
created_at          string   ,
text                string   ,
source              string   ,
geo                 string   ,
pic_urls            string   ,
uid                 string   ,
province            string   ,
city                string   ,
ulocation           string   ,
ugender             string   ,
ufollowers_count    string   ,
ufriends_count      string   ,
ustatuses_count     string   ,
ufavourites_count   string   ,
uverified           string   ,
verified_reason     string   ,
ubi_followers_count string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY'\t';

LOAD DATA LOCAL INPATH '/mnt/disk3/weibo_add/*/*.txt'  INTO TABLE extract.socialmedia_weibo_add_txt;


-- 预处理
drop table if exists transforms.socialmedia_weibo_add;
CREATE TABLE  if not exists transforms.socialmedia_weibo_add(
id  string,
platform_id  int,
post_id  string,
user_id  string,
user_name  string,
user_location  string,
transmit_post_id  string,
content  string,
content_type  int,
image_urls  string,
device  string,
verified_type  string,
comment_count  int,
like_count  int,
transmit_count  int,
detail_url  string,
updated_at  string,
created_at  string,
noise  int
)
PARTITIONED BY (daterange string)
STORED AS orc
;


insert into transforms.socialmedia_weibo_add PARTITION(daterange)
select
id,
3002 ,
id,
if(nvl(uid, '') != '', concat('3002', ":", uid), uid),
''                      ,
ulocation               ,
id,
text                    ,
0                       ,
pic_urls                ,
regexp_extract(source,'>(.*)<',1),
verified_reason,
''                      ,
''                      ,
''                      ,
''                      ,
from_unixtime(unix_timestamp(created_at,"EEE MMM dd HH:mm:ss z yyyy"),"yyyy-MM-dd HH:mm:ss"),
from_unixtime(unix_timestamp(created_at,"EEE MMM dd HH:mm:ss z yyyy"),"yyyy-MM-dd HH:mm:ss"),
'',
from_unixtime(unix_timestamp(created_at,"EEE MMM dd HH:mm:ss z yyyy"),"yyyyMM")
from extract.socialmedia_weibo_add_txt
where id != 'id'
;


-- 去水



---- 3、 微博新字典增量入库

-- 入库
drop table if exists extract.socialmedia_weibo_add_txt;
CREATE TABLE  if not exists extract.socialmedia_weibo_add_txt(
id string,
platform_id int,
post_id string,
user_id string,
user_name string,
user_location string,
transmit_post_id string,
content string,
content_type string,
image_urls string,
device string,
verified_type string,
comment_count string,
like_count string,
transmit_count string,
detail_url string,
updated_at string,
created_at string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY'\t';

LOAD DATA LOCAL INPATH '/mnt/disk3/weibo_new_dict_add/*/*.txt'  INTO TABLE extract.socialmedia_weibo_add_txt;

-- 预处理

drop table if exists transforms.socialmedia_weibo_add;
CREATE TABLE  if not exists transforms.socialmedia_weibo_add(
id  string,
platform_id  int,
post_id  string,
user_id  string,
user_name  string,
user_location  string,
transmit_post_id  string,
content  string,
content_type  int,
image_urls  string,
device  string,
verified_type  string,
comment_count  int,
like_count  int,
transmit_count  int,
detail_url  string,
updated_at  string,
created_at  string,
noise  int
)
PARTITIONED BY (daterange string)
STORED AS orc
;

insert into transforms.socialmedia_weibo_add PARTITION(daterange)
select
 id
,platform_id
,post_id
,user_id
,user_name
,user_location
,transmit_post_id
,content
,content_type
,image_urls
,device
,verified_type
,comment_count
,like_count
,transmit_count
,detail_url
,updated_at
,created_at
,''
from_unixtime(unix_timestamp(created_at,"yyyy-MM-dd HH:mm:ss"),"yyyyMM"),
extract.socialmedia_weibo_add_txt;


-- 去水









ADD FILE /home/udflib/udf_denoise_weibo_201703081414.py;
DROP TABLE IF EXISTS transforms.socialmedia_weibo_noise;
CREATE TABLE transforms.socialmedia_weibo_noise(id STRING, is_noise STRING) STORED AS ORC;
INSERT INTO transforms.socialmedia_weibo_noise
SELECT TRANSFORM(content, id) USING 'python udf_denoise_weibo_201703081414.py' AS (id, is_noise) FROM transforms.socialmedia_weibo;





---------------------------------------------------------------------------------------------------------------------- join --------------------------


drop table if exists l_socialmedia.weibo;
CREATE TABLE  if not exists l_socialmedia.weibo(
id  string,
platform_id  int,
post_id  string,
user_id  string,
user_name  string,
user_location  string,
transmit_post_id  string,
content  string,
content_type  int,
image_urls  string,
device  string,
verified_type  string,
comment_count  int,
like_count  int,
transmit_count  int,
detail_url  string,
updated_at  string,
created_at  string,
noise  int
)
PARTITIONED BY (daterange string)
STORED AS orc
;



INSERT INTO l_socialmedia.weibo  PARTITION(daterange)
SELECT
     s.id
    ,s.platform_id
    ,s.post_id
    ,if(s.user_id = concat(s.platform_id, ":"), '', user_id) AS user_id
    ,s.user_name
    ,s.user_location
    ,s.transmit_post_id
    ,s.content
    ,s.content_type
    ,s.image_urls
    ,s.device
    ,s.verified_type
    ,s.comment_count
    ,s.like_count
    ,s.transmit_count
    ,s.detail_url
    ,s.updated_at
    ,s.created_at
    ,CASE WHEN n.is_noise = "False" THEN '0' ELSE '1' END AS noise
    ,s.daterange
FROM transforms.socialmedia_weibo AS s
LEFT JOIN transforms.socialmedia_weibo_noise AS n
ON s.id = n.id
where daterange != '__HIVE_DEFAULT_PARTITION__'
;




------------------------------------------------------------去重-------------------------------------------------------------------


drop table if exists transforms.socialmedia_weibo_unique;
CREATE TABLE transforms.socialmedia_weibo_unique like l_socialmedia.weibo;
insert into table transforms.socialmedia_weibo_unique PARTITION(daterange)
select
     a.id
    ,a.platform_id
    ,a.post_id
    ,a.user_id
    ,a.user_name
    ,a.user_location
    ,a.transmit_post_id
    ,a.content
    ,a.content_type
    ,a.image_urls
    ,a.device
    ,a.verified_type
    ,a.comment_count
    ,a.like_count
    ,a.transmit_count
    ,a.detail_url
    ,a.updated_at
    ,a.created_at
    ,a.noise
    ,a.daterange
from
(
select t1.* ,ROW_NUMBER() OVER (Partition By id ORDER BY updated_at desc) AS rn
from l_socialmedia.weibo t1 where platform_id = 1001
) a
where a.rn=1
;



-----------------------------------------------------------------水军-----------------------------------------------------------------------


DROP TABLE IF EXISTS transforms.socialmedia_weibo_user_noise;
CREATE TABLE transforms.socialmedia_weibo_user_noise AS
SELECT
    user_id,
    COUNT(*) AS cnt
FROM l_medicine.weibo_unique
WHERE noise = '1'
GROUP BY
    user_id
HAVING COUNT(*) >= 5;



DROP TABLE IF EXISTS l_socialmedia.user_tmp;
CREATE TABLE l_socialmedia.user_tmp LIKE l_socialmedia.user;

INSERT INTO l_socialmedia.user_tmp
SELECT
    u.platform_id,
    u.user_id,
    u.user_name,
    u.user_real_name,
    u.user_location,
    u.user_gender,
    u.user_sex_orientation,
    u.user_relationship_status,
    u.user_birthday,
    u.user_level,
    u.blood_type,
    u.blog_url,
    u.weibo_personal_url,
    u.brief_intro,
    u.email,
    u.qq,
    u.msn,
    u.job_info,
    u.edu_info,
    u.tags,
    u.following_count,
    u.fans_count,
    u.blog_count,
    u.verified_intro,
    u.verified_type,
    u.detail_url,
    u.created_at,
    u.updated_at,
    CASE WHEN n.user_id IS NULL THEN 0 ELSE 1 END AS noise
FROM l_socialmedia.user AS u
LEFT JOIN transforms.socialmedia_weibo_user_noise AS n
ON u.user_id = n.user_id;


USE l_socialmedia;
DROP TABLE  IF EXISTS l_socialmedia.user_bak;
ALTER TABLE l_socialmedia.user RENAME TO l_socialmedia.user_bak;
ALTER TABLE l_socialmedia.user_tmp RENAME TO l_socialmedia.user;


