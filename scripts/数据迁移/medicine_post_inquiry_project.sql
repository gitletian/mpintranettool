--------------------------------------------------------------------------------------
----------                       医药问诊 入库 项目
---------- 1、医药问诊自动入库
----------
----------    cd /home/data/motherbaby/20170504
----------    for ZIP_DIC in `ls ./`; do echo $ZIP_DIC; cd  $ZIP_DIC; ls *.zip | xargs -i -t unzip {}; cd ..; done
----------    rm -rf  ./*/*.zip
----------    beeline -u jdbc:hive2://mphd02:10000 -n fengxiangqing --hivevar project='test_project' -f  /home/data/script/motherbaby_append.sql
----------
----------
--------------------------------------------------------------------------------------


drop table if exists extract.medicine_post_inquiry_txt_add;
CREATE TABLE  if not exists extract.medicine_post_inquiry_txt_add(
    id string,
    platform_id string,
    channel string,
    subject string,
    post_id string,
    title string,
    tags string,
    reply_count string,
    view_count string,
    collection_count string,
    detail_url string,
    content string,
    is_best_answer string,
    like_count string,
    user_id string,
    user_name string,
    user_type string,
    is_host string,
    replied_user_id string,
    replied_user_name string,
    created_at string,
    device string,
    updated_at string,
    baby_agethen string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY'\t';

LOAD DATA LOCAL INPATH '/home/data/medicine/${hivevar:project}/*/*.txt'  INTO TABLE extract.medicine_post_inquiry_txt_add;



----去重
drop table if exists transforms.medicine_post_inquiry_add_unique;
CREATE TABLE  if not exists transforms.medicine_post_inquiry_add_unique(
    id string,
    platform_id string,
    channel string,
    subject string,
    post_id string,
    title string,
    tags string,
    reply_count string,
    view_count string,
    collection_count string,
    detail_url string,
    content string,
    is_best_answer string,
    like_count string,
    user_id string,
    user_name string,
    user_type string,
    is_host string,
    replied_user_id string,
    replied_user_name string,
    created_at string,
    device string,
    updated_at string,
    baby_agethen string
)
STORED AS orc;
;

insert into table transforms.medicine_post_inquiry_add_unique
select
     t.id
    ,t.platform_id
    ,t.channel
    ,t.subject
    ,t.post_id
    ,t.title
    ,t.tags
    ,t.reply_count
    ,t.view_count
    ,t.collection_count
    ,t.detail_url
    ,t.content
    ,t.is_best_answer
    ,t.like_count
    ,t.user_id
    ,t.user_name
    ,t.user_type
    ,t.is_host
    ,t.replied_user_id
    ,t.replied_user_name
    ,t.created_at
    ,t.device
    ,t.updated_at
    ,t.baby_agethen
FROM
(
SELECT
m.*,
ROW_NUMBER() OVER ( Partition By m.id ORDER BY m.created_at desc) AS rn
FROM
extract.medicine_post_inquiry_txt_add m
) t where t.rn = 1
;



drop table if exists transforms.medicine_post_inquiry_add;
CREATE TABLE  if not exists transforms.medicine_post_inquiry_add(
    id     string,
    channel     int,
    subject     string,
    post_id     string,
    title     string,
    tags     string,
    reply_count     int,
    view_count     int,
    collection_count     int,
    detail_url     string,
    content     string,
    is_best_answer     int,
    like_count     int,
    user_id     string,
    user_name     string,
    user_type     int,
    is_host     int,
    replied_user_id     string,
    replied_user_name     string,
    created_at     string,
    device     string,
    updated_at     string,
    baby_agethen   string,
    baby_days   string,
    floorid     int,
    noise     int
)
PARTITIONED BY (platform_id string)
STORED AS orc;


insert into transforms.medicine_post_inquiry_add PARTITION(platform_id)
select
     t2.id
    ,t2.channel
    ,t2.subject
    ,t2.post_id
    ,t2.title
    ,t2.tags
    ,t2.reply_count
    ,t2.view_count
    ,t2.collection_count
    ,t2.detail_url
    ,t2.content
    ,t2.is_best_answer
    ,t2.like_count
    ,t2.user_id
    ,t2.user_name
    ,t2.user_type
    ,t2.is_host
    ,t2.replied_user_id
    ,t2.replied_user_name
    ,t2.created_at
    ,t2.device
    ,t2.updated_at
    ,t2.baby_agethen
    ,''
    ,0
    ,''
    ,t2.platform_id
from
transforms.medicine_post_inquiry_add_unique t2
where t2.is_host = 1
;


insert into transforms.medicine_post_inquiry_add PARTITION(platform_id)
select
     t2.id
    ,t2.channel
    ,t2.subject
    ,t2.post_id
    ,t2.title
    ,t2.tags
    ,t2.reply_count
    ,t2.view_count
    ,t2.collection_count
    ,t2.detail_url
    ,t2.content
    ,t2.is_best_answer
    ,t2.like_count
    ,t2.user_id
    ,t2.user_name
    ,t2.user_type
    ,t2.is_host
    ,t2.replied_user_id
    ,t2.replied_user_name
    ,t2.created_at
    ,t2.device
    ,t2.updated_at
    ,t2.baby_agethen
    ,''
    ,t2.rn
    ,''
    ,t2.platform_id
from
(
SELECT
t1.*, ROW_NUMBER() OVER ( Partition By platform_id, post_id ORDER BY created_at asc) AS rn
FROM
transforms.medicine_post_inquiry_add_unique t1
where t1.is_host != 1
) t2
;




-----------------------------------去水------------------------------------

drop table if exists transforms.medicine_post_inquiry_noise_add;
CREATE TABLE transforms.medicine_post_inquiry_noise_add(
id string,
is_noise string
)
STORED AS orc
;

add file /home/udflib/udf_denoise_inquiry_201703101802.py;


insert into table transforms.medicine_post_inquiry_noise_add
select
  TRANSFORM (content, id)
  USING 'python udf_denoise_inquiry_201703101802.py'
  AS (id, noise)
FROM
transforms.medicine_post_inquiry_add
;

--------------------------------------------join---------------------------------------------

drop table if exists l_medicine.project_${hivevar:project};
CREATE TABLE  if not exists l_medicine.project_${hivevar:project}(
    id   string,
    channel   int,
    subject   string,
    post_id   string,
    title   string,
    tags   string,
    reply_count   int,
    view_count   int,
    collection_count   int,
    detail_url   string,
    content   string,
    is_best_answer   int,
    like_count   int,
    user_id   string,
    user_name   string,
    user_type   int,
    is_host   int,
    replied_user_id   string,
    replied_user_name   string,
    created_at   string,
    device   string,
    updated_at   string,

    baby_agethen string,
    baby_days    string,

    floorid   string,
    noise   int
)
PARTITIONED BY (platform_id string)
STORED AS orc;



INSERT INTO l_medicine.project_${hivevar:project} PARTITION(platform_id)
SELECT
     s.id
    ,s.channel
    ,s.subject
    ,s.post_id
    ,s.title
    ,s.tags
    ,s.reply_count
    ,s.view_count
    ,s.collection_count
    ,s.detail_url
    ,s.content
    ,s.is_best_answer
    ,s.like_count
    ,if(s.user_id = concat(s.platform_id, ":"), '', user_id) AS user_id
    ,s.user_name
    ,s.user_type
    ,s.is_host
    ,s.replied_user_id
    ,s.replied_user_name
    ,s.created_at
    ,s.device
    ,s.updated_at
    ,s.baby_agethen
    ,s.baby_days
    ,s.floorid
    ,CASE WHEN n.is_noise = "False" THEN '0' ELSE '1' END AS noise
    ,s.platform_id
FROM
transforms.medicine_post_inquiry_add AS s
LEFT JOIN
transforms.medicine_post_inquiry_noise_add AS n
ON s.id = n.id
;



-----------------------------------------------------------------水军-----------------------------------------------------------------------


DROP TABLE IF EXISTS transforms.medicine_inquiry_user_noise;

with post as (
select user_id from l_medicine.post_inquiry where noise = '1'
UNION ALL
select user_id from l_medicine.project_${hivevar:project} where noise = '1'
)
CREATE TABLE transforms.medicine_inquiry_user_noise AS
SELECT
    user_id,
    COUNT(*) AS cnt
FROM post
GROUP BY
    user_id
HAVING COUNT(*) >= 5
;


DROP TABLE IF EXISTS l_medicine.user_tmp;
CREATE TABLE l_medicine.user_tmp LIKE l_medicine.user;

INSERT INTO l_medicine.user_tmp PARTITION (platform_id)
SELECT
    u.user_id,
    u.brief_intro,
    u.user_tags,
    u.user_name,
    u.detail_url,
    u.user_gender,
    u.user_birthday,
    u.user_age,
    u.user_level,
    u.baby_count,
    u.baby_info,
    u.baby_gender,
    u.baby_birthday,
    u.baby_agenow,
    u.ask_count,
    u.reply_count,
    u.post_count,
    u.reply_post_count,
    u.quality_post_count,
    u.best_answer_count,
    u.fans_count,
    u.following_count,
    u.device,
    u.address,
    u.tel,
    u.province,
    u.city,
    u.created_at,
    u.updated_at,
    CASE WHEN n.user_id IS NULL THEN 0 ELSE 1 END AS noise,
    u.platform_id
FROM l_medicine.user AS u
LEFT JOIN transforms.medicine_inquiry_user_noise AS n
ON u.user_id = n.user_id;


USE l_medicine;
DROP TABLE  IF EXISTS l_medicine.user_bak;
ALTER TABLE l_medicine.user RENAME TO l_medicine.user_bak;
ALTER TABLE l_medicine.user_tmp RENAME TO l_medicine.user;

