--------------------------------------------------------------------------------------
----------                       weibo 数据迁移及定时入库
---------- 1、医药原始数据迁移
----------  1.1 问诊
----------  1.2 非问诊
---------- 2、医药新字典增量入库
----------
--------------------------------------------------------------------------------------

---- 1、 医药原始数据迁移

set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;

---------- 1.1、问诊
-- 入库
drop table if exists extract.medicine_post_inquiry_txt;
CREATE TABLE  if not exists extract.medicine_post_inquiry_txt(
platform           string,
channel            int,
subject            string,
contenttype        int,
isbestanswer       string,
ishost             string,
url                string,
postid             string,
floorid            int,
title              string,
content            string,
tags               string,
userid             string,
usertype           int,
username           string,
userprofileurl     string,
gender             string,
birthday           string,
userlevel          string,
location           string,
babybirthday       string,
babyagethen        string,
postdate           string,
userstate          string,
replycount         string,
viewcount          string,
collectioncount    string,
device             string,
hospital           string,
department         string,
section            string,
jobtitle           string,
academictitle      string,
speciality         string,
likes              string,
crawldate          string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY'\t';

LOAD DATA LOCAL INPATH '/mnt/disk6/mp_hcp/post_inquiry/*/*'  INTO TABLE extract.medicine_post_inquiry_txt;


-- 预处理
drop table if exists transforms.medicine_post_inquiry;
CREATE TABLE  if not exists transforms.medicine_post_inquiry(
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


insert into transforms.medicine_post_inquiry PARTITION(platform_id)
select
reflect("java.util.UUID", "randomUUID"),
a.channel         ,
a.subject         ,
if(a.platform = '2001', regexp_replace(a.postid, '-', '_'), a.postid),
a.title           ,
a.tags            ,
a.replycount      ,
a.viewcount       ,
a.collectioncount ,
if(a.platform = '2001',regexp_replace(a.url, '-', '_') , a.url),
a.content         ,
case
    when nvl(a.isbestanswer, '') in ('0', 'n', '') then 0
    when nvl(a.isbestanswer, '') in ('y', '1') then 1
    else ''
end as isbestanswer,
a.likes           ,
if(nvl(userid, '') != '', concat(platform, ":", userid), userid),
a.username        ,
a.usertype        ,
case
    when ishost = 'n' or ishost = 0 then 0
    when ishost = 'y' or ishost = 1 then 1
    when channel = 1 and floorid = 0 then 1
    when channel = 1 and floorid != 0 then 0
    when channel = 2 and floorid = 1 then 1
    when channel = 2 and floorid != 1 then 0
    else '10000'
end as ishost,

''                ,
''                ,
case
    when length(a.postdate) = 10 then concat(a.postdate, " 00:00:00")
    when length(a.postdate) = 16 then concat(a.postdate, ":00")
    when length(a.postdate) = 19 then a.postdate
else a.postdate
end,
a.device          ,
case
    when length(a.crawldate) = 10 then concat(a.crawldate, " 00:00:00")
    when length(a.crawldate) = 16 then concat(a.crawldate, ":00")
    when length(a.crawldate) = 19 then a.crawldate
else a.crawldate
end,
babyagethen       ,
''                ,
a.floorid         ,
''                ,
a.platform

from
extract.medicine_post_inquiry_txt a
where
length(nvl(a.platform, '')) = 4
and nvl(a.channel, '') != ''
and nvl(a.postid, '') != ''
and (nvl(title, '') != '' or nvl(content, '') != '')
and nvl(a.url, '') != ''
and nvl(a.isbestanswer, '') in ('1', '0', 'y', 'n', '')
and nvl(a.usertype, '') != ''
and length(nvl(a.crawldate, '')) > 9
and length(nvl(a.postdate, '')) > 9
;



drop table if exists transforms.medicine_post_inquiry_error;
CREATE TABLE  if not exists transforms.medicine_post_inquiry_error
STORED AS orc
as
select
*
from extract.medicine_post_inquiry_txt
where
length(nvl(platform, '')) != 4
or nvl(channel, '') = ''
or nvl(postid, '') = ''
or ((nvl(title, '') = '' and nvl(content, '') = ''))
or nvl(url, '') = ''
or nvl(isbestanswer, '') not in ('1', '0', 'y', 'n', '')
or nvl(usertype, '') = ''
or length(nvl(crawldate, '')) < 10
or length(nvl(postdate, '')) < 10
;


insert overwrite local directory "/home/data/tmp/medicine_post_inquiry_error"
row format delimited
fields terminated by "\t"
select * from transforms.medicine_post_inquiry_error order by platform desc
;


-------- 去水



drop table if exists transforms.medicine_post_inquiry_noise_1;
CREATE TABLE transforms.medicine_post_inquiry_noise_1 like transforms.medicine_post_inquiry_noise;


add file /home/udflib/udf_denoise_inquiry_201703101802.py;
insert into table transforms.medicine_post_inquiry_noise_1
select
  TRANSFORM (content, id)
  USING 'python udf_denoise_inquiry_201703101802.py'
  AS (id, noise)
FROM
transforms.medicine_post_inquiry_1
;






---------- 1.2、非问诊

-- 入库
drop table if exists extract.medicine_post_txt;
CREATE TABLE  if not exists extract.medicine_post_txt(
platform           string,
channel            int,
subject            string,
contenttype        int,
isbestanswer       string,
ishost             string,
url                string,
postid             string,
floorid            int,
title              string,
content            string,
tags               string,
userid             string,
usertype           int,
username           string,
userprofileurl     string,
gender             string,
birthday           string,
userlevel          string,
location           string,
babybirthday       string,
babyagethen        string,
postdate           string,
userstate          string,
replycount         string,
viewcount          string,
collectioncount    string,
device             string,
hospital           string,
department         string,
section            string,
jobtitle           string,
academictitle      string,
speciality         string,
likes              string,
crawldate          string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY'\t';

LOAD DATA LOCAL INPATH '/mnt/disk6/mp_hcp/post/*/*'  INTO TABLE extract.medicine_post_txt;

-- 预处理
drop table if exists transforms.medicine_post;
CREATE TABLE  if not exists transforms.medicine_post(
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




insert into transforms.medicine_post PARTITION(platform_id)
select
reflect("java.util.UUID", "randomUUID"),
channel         ,
subject         ,
if(platform = '2001', regexp_replace(postid, '-', '_'), postid),
title           ,
tags            ,
replycount      ,
viewcount       ,
collectioncount ,
if(platform = '2001',regexp_replace(url, '-', '_') , url),
content         ,
case
    when nvl(isbestanswer, '') in ('0', 'n', '') then 0
    when nvl(isbestanswer, '') in ('y', '1') then 1
    else ''
end as isbestanswer,
likes           ,
if(nvl(userid, '') != '', concat(platform, ":", userid), userid),
username        ,
usertype        ,
case
    when ishost = 'n' or ishost = 0 then 0
    when ishost = 'y' or ishost = 1 then 1
    else ''
end as ishost,
''              ,
''              ,
from_unixtime(unix_timestamp(concat(postdate, ":00"),"yyyy-M-d HH:mm:ss"),"yyyy-MM-dd HH:mm:ss"),
device          ,
from_unixtime(unix_timestamp(crawldate,"yyyy-MM-dd HH:mm:ss"),"yyyy-MM-dd HH:mm:ss"),
babyagethen     ,
''              ,
if(platform = '2006', floorid - 1, floorid),
'',
platform
from extract.medicine_post_txt
where
length(nvl(platform, '')) = 4
and nvl(channel, '') != ''
and nvl(postid, '') != ''
and (nvl(title, '') != '' or nvl(content, '') != '')
and nvl(url, '') != ''
and nvl(isbestanswer, '') in ('1', '0', 'y', 'n', '')
and nvl(usertype, '') != ''
and nvl(ishost, '') in ('1', '0', 'y', 'n')
and nvl(crawldate, '') != ''
and length(nvl(postdate, '')) in (14, 15, 16)
and nvl(floorid, '') != ''
;



drop table if exists transforms.medicine_post_error;
CREATE TABLE  if not exists transforms.medicine_post_error
STORED AS orc
as
select
*
from extract.medicine_post_txt
where
length(nvl(platform, '')) != 4
or nvl(channel, '') = ''
or nvl(postid, '') = ''
or ((nvl(title, '') = '' and nvl(content, '') = ''))
or nvl(url, '') = ''
or nvl(isbestanswer, '') not in ('1', '0', 'y', 'n', '')
or nvl(usertype, '') = ''
or nvl(ishost, '') not in ('1', '0', 'y', 'n')
or nvl(crawldate, '') = ''
or length(nvl(postdate, '')) not in (14, 15, 16)
or nvl(floorid, '') = ''
;


insert overwrite local directory "/home/data/tmp/medicine_post_error"
row format delimited
fields terminated by "\t"
select * from transforms.medicine_post_error order by platform desc
;





-------- 2、医药新字典增量入库

---- 问诊 和 非问诊  (问政和非问诊处理方式相同)

-- 入库
drop table if exists extract.medicine_add_txt;
CREATE TABLE  if not exists extract.medicine_add_txt(
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

LOAD DATA LOCAL INPATH '/mnt/disk3/medicine_post_new_dict_add/*/*.txt'  INTO TABLE extract.medicine_add_txt;


-- 预处理
drop table if exists transforms.medicine_add;
CREATE TABLE  if not exists transforms.medicine_add(
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


insert into transforms.medicine_add PARTITION(platform_id)
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
,created_at
,device
,updated_at
,baby_agethen
,''
,''
,''
,platform_id
from
extract.medicine_add_txt;


-- 去水






---------------------------------------------------------------------------------------------------------------------- join --------------------------



drop table if exists l_medicine.post_inquiry;
CREATE TABLE  if not exists l_medicine.post_inquiry(
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



INSERT INTO l_medicine.post_inquiry PARTITION(platform_id)
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
FROM transforms.medicine_post_inquiry AS s
LEFT JOIN transforms.medicine_post_inquiry_noise AS n
ON s.id = n.id
;





drop table if exists l_medicine.post;
create table l_medicine.post like transforms.medicine_post;
insert into table l_medicine.post PARTITION(platform_id)
select
*
from
transforms.medicine_post
;





------------------------------------------------去重------------------------------------------------

drop table if exists transforms.medicine_post_inquiry_unique;
CREATE TABLE transforms.medicine_post_inquiry_unique like l_medicine.post_inquiry;
insert into table transforms.medicine_post_inquiry_unique PARTITION(platform_id)
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
from l_medicine.post_inquiry t1
) a
where a.rn=1
;



drop table if exists transforms.medicine_post_unique;
CREATE TABLE transforms.medicine_post_unique like transforms.medicine_post;
insert into table transforms.medicine_post_unique PARTITION(platform_id)
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
from transforms.medicine_post t1 where nvl(post_id, '') != ''
) a
where a.rn=1
;




-----------------------------------------------------------------水军-----------------------------------------------------------------------


DROP TABLE IF EXISTS transforms.medicine_inquiry_user_noise;
CREATE TABLE transforms.medicine_inquiry_user_noise AS
SELECT
    user_id,
    COUNT(*) AS cnt
FROM l_medicine.post_inquiry_unique
WHERE noise = '1'
GROUP BY
    user_id
HAVING COUNT(*) >= 5;



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







---------------------------------------------数据校验-----------------------------------------

select
  count(if(length(nvl(platform, '')) != 4, 1, null)),
  count(if(nvl(channel, '') = '', 1, null)),
  count(if(nvl(postid, '') = '', 1, null)),
  count(if(((nvl(title, '') = '' and nvl(content, '') = '')), 1, null)),
  count(if(nvl(url, '') = '', 1, null)),
  count(if(nvl(isbestanswer, '') not in ('1', '0', 'y', 'n', ''), 1, null)),
  count(if(nvl(usertype, '') = '', 1, null)),
  count(if(nvl(ishost, '') not in ('1', '0', 'y', 'n'), 1, null)),
  count(if(nvl(crawldate, '') = '', 1, null)),
  count(if(length(nvl(postdate, '')) not in (14, 15, 16), 1, null)),
  count(if(nvl(floorid, '') = '', 1, null))

  from extract.medicine_post_txt_add;



select
count(if( length(nvl(platform, '')) != 4, 1, null)),
count(if( nvl(channel, '') = '', 1, null)),
count(if( nvl(postid, '') = '', 1, null)),
count(if( ((nvl(title, '') = '' and nvl(content, '') = '')), 1, null)),
count(if( nvl(url, '') = '', 1, null)),
count(if( nvl(isbestanswer, '') not in ('1', '0', 'y', 'n', ''), 1, null)),
count(if( nvl(usertype, '') = '', 1, null)),
count(if( length(nvl(crawldate, '')) < 10, 1, null)),
count(if( length(nvl(postdate, '')) < 10, 1, null))
from extract.medicine_post_inquiry_txt_add
;

