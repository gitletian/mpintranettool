--------------------------------------------------------------------------------------
----------                       weibo 数据迁移及定时入库
---------- 1、母婴原始数据迁移
---------- 2、母婴新字典增量入库
----------
--------------------------------------------------------------------------------------

---- 1、 母婴原始数据迁移

set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
-- 入库
drop table if exists extract.motherbaby_post_txt;
CREATE TABLE  if not exists extract.motherbaby_post_txt(
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

LOAD DATA LOCAL INPATH '/mnt/disk4/health_mother_inquiry/*/*'  INTO TABLE extract.motherbaby_post_txt;
LOAD DATA LOCAL INPATH '/mnt/disk5/health_mother_inquiry/*/*'  INTO TABLE extract.motherbaby_post_txt;


-- 预处理
drop table if exists transforms.motherbaby_post;
CREATE TABLE  if not exists transforms.motherbaby_post(
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




add file /home/guoyuanpei/mpintranet/gestation_compute_new_3.py;

insert into transforms.motherbaby_post PARTITION(platform_id)
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
USING "python gestation_compute_new_3.py"
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
from extract.motherbaby_post_txt
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





drop table if exists transforms.motherbaby_post_error;
CREATE TABLE  if not exists transforms.motherbaby_post_error(
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
CLUSTERED BY (url) INTO 31 BUCKETS
STORED AS ORC;


insert into table transforms.motherbaby_post_error
select
*
from extract.motherbaby_post_txt
where
length(nvl(platform, '')) != 4
or nvl(channel, '') = ''
or nvl(postid, '') = ''
or ((nvl(title, '') = '' and nvl(content, '') = ''))
or nvl(url, '') = ''
or nvl(isbestanswer, '') not in ('1', '0', 'y', 'n', '')
or length(nvl(crawldate, '')) < 10
or length(nvl(postdate, '')) < 10
;

insert overwrite local directory "/home/data/tmp/motherbaby_post_error"
row format delimited
fields terminated by "\t"
select * from transforms.motherbaby_post_error;

-- 去水







---- 2、 母婴新字典增量入库

-- 入库
drop table if exists extract.motherbaby_post_add_txt;
CREATE TABLE  if not exists extract.motherbaby_post_add_txt(
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

LOAD DATA LOCAL INPATH '/mnt/disk3/medicine_post_new_dict_add/*/*.txt'  INTO TABLE extract.motherbaby_post_add_txt;

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

add file /home/guoyuanpei/mpintranet/gestation_compute_new_dict_2.py;

insert into transforms.motherbaby_post_add PARTITION(platform_id)
select
TRANSFORM (
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
    ,platform_id
)
USING "python gestation_compute_new_dict_2.py"
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
from extract.motherbaby_post_add_txt
;

-- insert into transforms.motherbaby_post PARTITION(platform_id) select * from transforms.motherbaby_post_add;
-- 去水






---------------------------------------------------------------------------------------------------------------------- join --------------------------



drop table if exists l_motherbaby.post;
CREATE TABLE  if not exists l_motherbaby.post(
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



INSERT INTO l_motherbaby.post PARTITION(platform_id)
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
FROM transforms.motherbaby_post AS s
LEFT JOIN transforms.motherbaby_post_noise AS n
ON s.id = n.id;


---------------------------------------------------------------------------------------------------------------------- 去重
drop table if exists transforms.motherbaby_post_unique;
CREATE TABLE transforms.motherbaby_post_unique like l_motherbaby.post;
insert into table transforms.motherbaby_post_unique PARTITION(platform_id)
select
 a.id
,a.channel
,a.subject
,a.post_id
,a.title
,a.tags
,a.reply_count
,a.view_count
,a.collection_count
,a.detail_url
,a.content
,a.is_best_answer
,a.like_count
,a.user_id
,a.user_name
,a.user_type
,a.is_host
,a.replied_user_id
,a.replied_user_name
,a.created_at
,a.device
,a.updated_at
,a.baby_agethen
,a.baby_days
,a.floorid
,a.noise
,a.platform_id
from
(
select t1.* ,ROW_NUMBER() OVER (Partition By concat(platform_id, post_id, floorid, channel) ORDER BY updated_at desc) AS rn
from l_motherbaby.post t1 where platform_id = 1001
) a
where a.rn=1
;



insert into table transforms.motherbaby_post_unique PARTITION(platform_id)
select
 a.id
,a.channel
,a.subject
,a.post_id
,a.title
,a.tags
,a.reply_count
,a.view_count
,a.collection_count
,a.detail_url
,a.content
,a.is_best_answer
,a.like_count
,a.user_id
,a.user_name
,a.user_type
,a.is_host
,a.replied_user_id
,a.replied_user_name
,a.created_at
,a.device
,a.updated_at
,a.baby_agethen
,a.baby_days
,a.floorid
,a.noise
,a.platform_id
from
l_motherbaby.post a
where a.platform_id != 1001
;











-------------------------修复---------------




drop table if exists transforms.medicine_post_inquiry_1;
create table transforms.medicine_post_inquiry_1 like transforms.medicine_post_inquiry;
insert into table transforms.medicine_post_inquiry_1 PARTITION(platform_id)
select
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
,if(created_at regexp ':00:00:00', concat(substr(created_at, 0, 10), " 00:00:00"), created_at)
,device
,if(updated_at regexp ':00:00:00', concat(substr(updated_at, 0, 10), " 00:00:00"), updated_at)
,baby_agethen
,baby_days
,floorid
,noise
,platform_id
from
transforms.medicine_post_inquiry;




drop table if exists l_medicine.post_inquiry_1;
create table l_medicine.post_inquiry_1 like l_medicine.post_inquiry;
insert into table l_medicine.post_inquiry_1 PARTITION(platform_id)
select
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
,if(created_at regexp ':00:00:00', concat(substr(created_at, 0, 10), " 00:00:00"), created_at)
,device
,if(updated_at regexp ':00:00:00', concat(substr(updated_at, 0, 10), " 00:00:00"), updated_at)
,baby_agethen
,baby_days
,floorid
,noise
,platform_id
from
l_medicine.post_inquiry;



drop table if exists l_medicine.post_inquiry_unique_1;
create table l_medicine.post_inquiry_unique_1 like l_medicine.post_inquiry_unique;
insert into table l_medicine.post_inquiry_unique_1 PARTITION(platform_id)
select
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
,if(created_at regexp ':00:00:00', concat(substr(created_at, 0, 10), " 00:00:00"), created_at)
,device
,if(updated_at regexp ':00:00:00', concat(substr(updated_at, 0, 10), " 00:00:00"), updated_at)
,baby_agethen
,baby_days
,floorid
,noise
,platform_id
from
l_medicine.post_inquiry_unique;








drop table if exists transforms.motherbaby_post_1;
create table transforms.motherbaby_post_1 like transforms.motherbaby_post;
insert into table transforms.motherbaby_post_1 PARTITION(platform_id)
select
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
,if(created_at regexp ':00:00:00', concat(substr(created_at, 0, 10), " 00:00:00"), created_at)
,device
,if(updated_at regexp ':00:00:00', concat(substr(updated_at, 0, 10), " 00:00:00"), updated_at)
,baby_agethen
,baby_days
,floorid
,noise
,error_info
,platform_id
from
transforms.motherbaby_post;




drop table if exists l_motherbaby.post_1;
create table l_motherbaby.post_1 like l_motherbaby.post;
insert into table l_motherbaby.post_1 PARTITION(platform_id)
select
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
,if(created_at regexp ':00:00:00', concat(substr(created_at, 0, 10), " 00:00:00"), created_at)
,device
,if(updated_at regexp ':00:00:00', concat(substr(updated_at, 0, 10), " 00:00:00"), updated_at)
,baby_agethen
,baby_days
,floorid
,noise
,platform_id
from
l_motherbaby.post;



drop table if exists l_motherbaby.post_unique_1;
create table l_motherbaby.post_unique_1 like l_motherbaby.post_unique;
insert into table l_motherbaby.post_unique_1 PARTITION(platform_id)
select
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
,if(created_at regexp ':00:00:00', concat(substr(created_at, 0, 10), " 00:00:00"), created_at)
,device
,if(updated_at regexp ':00:00:00', concat(substr(updated_at, 0, 10), " 00:00:00"), updated_at)
,baby_agethen
,baby_days
,floorid
,noise
,platform_id
from
l_motherbaby.post_unique;



-----------------------------------------------------------------水军-----------------------------------------------------------------------


DROP TABLE IF EXISTS transforms.motherbaby_user_noise;
CREATE TABLE transforms.motherbaby_user_noise AS
SELECT
    user_id,
    COUNT(*) AS cnt
FROM l_motherbaby.post_unique
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




---------------------------------------------数据校验-----------------------------------------
select
count( if(length(nvl(platform, '')) != 4, 1, null)),
count( if(nvl(channel, '') = '', 1, null)),
count( if(nvl(postid, '') = '', 1, null)),
count( if(((nvl(title, '') = '' and nvl(content, '') = '')), 1, null)),
count( if(nvl(url, '') = '', 1, null)),
count( if(nvl(isbestanswer, '') not in ('1', '0', 'y', 'n', ''), 1, null)),
count( if(length(nvl(crawldate, '')) < 10, 1, null)),
count( if(length(nvl(postdate, '')) < 10, 1, null))
from extract.motherbaby_post_txt_add
;