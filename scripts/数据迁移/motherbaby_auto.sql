--------------------------------------------------------------------------------------
----------                       母婴自动入库
---------- 1、母婴自动入库
----------
----------    cd /home/data/motherbaby/20170504
----------    for ZIP_DIC in `ls ./`; do echo $ZIP_DIC; cd  $ZIP_DIC; ls *.zip | xargs -i -t unzip {}; cd ..; done
----------    rm -rf  ./*/*.zip
------------------------------------------旧字典-------------------------------------------
drop table if exists extract.motherbaby_post_txt_add;
CREATE TABLE  if not exists extract.motherbaby_post_txt_add(
platform               string,
channel                int,
subject                string,
contenttype            int,
isbestanswer           string,
ishost                 string,
url                    string,
postid                 string,
floorid                int,
title                  string,
content                string,
tags                   string,
userid                 string,
usertype               int,
username               string,
userprofileurl         string,
gender                 string,
birthday               string,
userlevel              string,
location               string,
babybirthday           string,
babyagethen            string,
postdate               string,
userstate              string,
replycount             string,
viewcount              string,
collectioncount        string,
device                 string,
hospital               string,
department             string,
section                string,
jobtitle               string,
academictitle          string,
speciality             string,
likes                  string,
crawldate              string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY'\t';

LOAD DATA LOCAL INPATH '/home/data/motherbaby/20170505/1004_old/*.txt'  INTO TABLE extract.motherbaby_post_txt_add;


-- 预处理
drop table if exists transforms.motherbaby_post_add;
CREATE TABLE  if not exists transforms.motherbaby_post_add(
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
noise   int,
error_info string
)
PARTITIONED BY (platform_id string)
STORED AS orc;




add file /home/script/test/mpintranet/gestation_compute_post_8.py;

insert into transforms.motherbaby_post_add PARTITION(platform_id)
select
TRANSFORM (
    reflect("java.util.UUID", "randomUUID")
    ,channel
    ,subject
    ,postid
    ,case
        when nvl(title, '') != '' then title
        when nvl(ishost, '') in ('y', '1') or (nvl(channel, 10000) = 1 and nvl(floorid, 10000) = 0) or (nvl(channel, 10000) = 2 and nvl(floorid, 10000) = 1) then content
    end
    ,tags
    ,replycount
    ,viewcount
    ,collectioncount
    ,url
    ,content
    ,case
        when nvl(isbestanswer, '') in ('0', 'n', '') then 0
        when nvl(isbestanswer, '') in ('y', '1') then 1
        else ''
    end
    ,likes
    ,if(nvl(userid, '') != '', concat(platform, ":", userid), userid)
    ,username
    ,if(nvl(usertype, '') = '', 1, usertype)

    ,case
        when nvl(ishost, '') in ('n', '0') then 0
        when nvl(ishost, '') in ('y', '1') then 1
        when channel = 1 and floorid = 0 then 1
        when channel = 1 and floorid != 0 then 0
        when channel = 2 and floorid = 1 then 1
        when channel = 2 and floorid != 1 then 0
        else '10000'
    end

    ,''
    ,''
    ,case
        when length(postdate) = 10 then concat(postdate, " 00:00:00")
        when length(postdate) = 16 then concat(postdate, ":00")
        when length(postdate) = 19 then postdate
        else ''
    end
    ,device
    ,case
        when length(crawldate) = 10 then concat(crawldate, " 00:00:00")
        when length(crawldate) = 16 then concat(crawldate, ":00")
        else crawldate
    end

    ,babybirthday
    ,babyagethen

    ,floorid

    ,''
    ,platform
)
USING "python gestation_compute_post_8.py"
AS (
     id
    ,channel
    ,subject
    ,post_id
    ,title
    ,tags
    ,reply_count
    ,view_count
    ,collection_count
    ,detail_url
    ,content
    ,is_best_answer
    ,like_count
    ,user_id
    ,user_name
    ,user_type
    ,is_host
    ,replied_user_id
    ,replied_user_name
    ,created_at
    ,device
    ,updated_at

    ,baby_agethen
    ,baby_days

    ,floorid
    ,noise
    ,error_info
    ,platform_id
)
from extract.motherbaby_post_txt_add
where
length(nvl(platform, '')) = 4
and nvl(channel, '') != ''
and nvl(postid, '') != ''
and (nvl(title, '') != '' or nvl(content, '') != '')
and nvl(url, '') != ''
and nvl(isbestanswer, '') in ('1', '0', 'y', 'n', '')
and length(nvl(crawldate, '')) > 9
and length(nvl(postdate, '')) > 9
;


------------------------------------------新字典-------------------------------------------
---- 1、 母婴原始数据迁移

-- 入库
drop table if exists extract.motherbaby_post_txt_add;
CREATE TABLE  if not exists extract.motherbaby_post_txt_add(
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

LOAD DATA LOCAL INPATH '/home/data/motherbaby/${hivevar:date_range}/*/*.txt'  INTO TABLE extract.motherbaby_post_txt_add;

----去重
drop table if exists transforms.motherbaby_post_add_unique;
CREATE TABLE  if not exists transforms.motherbaby_post_add_unique(
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
CLUSTERED BY (id) SORTED BY (id) INTO 113 BUCKETS
STORED AS orc

;

insert into table transforms.motherbaby_post_add_unique
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
ROW_NUMBER() OVER ( Partition By m.id ORDER BY m.created_at asc) AS rn
FROM
extract.motherbaby_post_txt_add m
) t where t.rn = 1
;


-- 预处理

drop table if exists transforms.motherbaby_post_add;
CREATE TABLE  if not exists transforms.motherbaby_post_add(
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
noise   int,
error_info string
)
PARTITIONED BY (platform_id string)
CLUSTERED BY (id) SORTED BY (id) INTO 113 BUCKETS
STORED AS orc;


add file /home/script/test/mpintranet/gestation_compute_post_1.py;


insert into transforms.motherbaby_post_add PARTITION(platform_id)
select
TRANSFORM (
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
    ,""

    ,0
    ,''
    ,t2.platform_id
)
USING "python gestation_compute_post_1.py"
AS (
     id
    ,channel
    ,subject
    ,post_id
    ,title
    ,tags
    ,reply_count
    ,view_count
    ,collection_count
    ,detail_url
    ,content
    ,is_best_answer
    ,like_count
    ,user_id
    ,user_name
    ,user_type
    ,is_host
    ,replied_user_id
    ,replied_user_name
    ,created_at
    ,device
    ,updated_at

    ,baby_agethen
    ,baby_days

    ,floorid
    ,noise
    ,error_info
    ,platform_id
)
from
transforms.motherbaby_post_add_unique  t2
where t2.is_host = 1
;



insert into transforms.motherbaby_post_add PARTITION(platform_id)
select
TRANSFORM (
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
    ,""

    ,t2.rn
    ,''
    ,t2.platform_id
)
USING "python gestation_compute_post_1.py"
AS (
     id
    ,channel
    ,subject
    ,post_id
    ,title
    ,tags
    ,reply_count
    ,view_count
    ,collection_count
    ,detail_url
    ,content
    ,is_best_answer
    ,like_count
    ,user_id
    ,user_name
    ,user_type
    ,is_host
    ,replied_user_id
    ,replied_user_name
    ,created_at
    ,device
    ,updated_at

    ,baby_agethen
    ,baby_days

    ,floorid
    ,noise
    ,error_info
    ,platform_id
)
from
(
SELECT
t1.*, ROW_NUMBER() OVER ( Partition By post_id ORDER BY created_at asc) AS rn
FROM
transforms.motherbaby_post_add_unique t1 where t1.is_host != 1
) t2
;

----------------------------------------------join user----------------------------------------------

drop table if exists transforms.post_join_user;
CREATE TABLE transforms.post_join_user(
  id                 string
 ,channel            string
 ,subject            string
 ,post_id            string
 ,title              string
 ,tags               string
 ,reply_count        string
 ,view_count         string
 ,collection_count   string
 ,detail_url         string
 ,content            string
 ,is_best_answer     string
 ,like_count         string
 ,user_id            string
 ,user_name          string
 ,user_type          string
 ,is_host            string
 ,replied_user_id    string
 ,replied_user_name  string
 ,created_at         string
 ,device             string
 ,updated_at         string
 ,baby_agethen       string
 ,baby_days          string
 ,floorid            string
 ,noise              string
)
PARTITIONED BY (platform_id string)
CLUSTERED BY (id) SORTED BY (id) INTO 537 BUCKETS
STORED AS ORC
;


insert into transforms.post_join_user PARTITION(platform_id)
select
 t1.id
,t1.channel
,t1.subject
,t1.post_id
,t1.title
,t1.tags
,t1.reply_count
,t1.view_count
,t1.collection_count
,t1.detail_url
,t1.content
,t1.is_best_answer
,t1.like_count
,t1.user_id
,t1.user_name
,t1.user_type
,t1.is_host
,t1.replied_user_id
,t1.replied_user_name
,t1.created_at
,t1.device
,t1.updated_at
,t1.baby_agethen

,if(nvl(to_date(t2.baby_birthday), '') != '' and nvl(t1.baby_days, '') in ('', 'error', '未知', '怀孕中', '已有宝宝'), datediff(to_date(t1.created_at), to_date(t2.baby_birthday)), t1.baby_days)

,t1.floorid
,t1.noise
,t1.platform_id
from
  transforms.motherbaby_post_add t1
left join
l_motherbaby.user t2
on t1.user_id = t2.user_id and t1.platform_id = t2.platform_id
-- where t1.platform_id in (1001, 1003)
;



----------------------------------------------去水----------------------------------------------

drop table if exists transforms.motherbaby_post_noise_add;
CREATE TABLE transforms.motherbaby_post_noise_add like transforms.motherbaby_post_noise;


add file /home/udflib/udf_denoise_motherbaby_201703161633.py;


insert into table transforms.motherbaby_post_noise_add
select
  TRANSFORM (content, id)
  USING 'python udf_denoise_motherbaby_201703161633.py'
  AS (id, noise)
FROM
transforms.post_join_user
;


---------------------------------------------------join----------------------------------------------------------------
drop table if exists transforms.motherbaby_post_join_noise_add;
CREATE TABLE  if not exists transforms.motherbaby_post_join_noise_add(
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



INSERT INTO transforms.motherbaby_post_join_noise_add PARTITION(platform_id)
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
    ,s.user_id
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
FROM transforms.post_join_user AS s
LEFT JOIN transforms.motherbaby_post_noise_add AS n
ON s.id = n.id
;



------------------合并---------------------
insert into table l_motherbaby.post PARTITION(platform_id) select * from transforms.motherbaby_post_join_noise_add;


-----------------------------------------------------------------水军-----------------------------------------------------------------------


DROP TABLE IF EXISTS transforms.motherbaby_user_noise;
CREATE TABLE transforms.motherbaby_user_noise AS
SELECT
    user_id,
    COUNT(*) AS cnt
FROM l_motherbaby.post
WHERE noise = '1'
GROUP BY
    user_id
HAVING COUNT(*) >= 5
;



DROP TABLE IF EXISTS l_motherbaby.user_tmp;
CREATE TABLE l_motherbaby.user_tmp LIKE l_motherbaby.user;

INSERT INTO l_motherbaby.user_tmp PARTITION (platform_id)
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
FROM l_motherbaby.user AS u
LEFT JOIN transforms.motherbaby_user_noise AS n
ON u.user_id = n.user_id;


USE l_motherbaby;
DROP TABLE  IF EXISTS l_motherbaby.user_bak;
ALTER TABLE l_motherbaby.user RENAME TO l_motherbaby.user_bak;
ALTER TABLE l_motherbaby.user_tmp RENAME TO l_motherbaby.user;


