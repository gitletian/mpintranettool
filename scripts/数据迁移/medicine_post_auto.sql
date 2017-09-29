--------------------------------------------------------------------------------------
----------                       医药问诊自动入库
---------- 1、医药问诊自动入库
----------
----------    cd /home/data/motherbaby/20170504
----------    for ZIP_DIC in `ls ./`; do echo $ZIP_DIC; cd  $ZIP_DIC; ls *.zip | xargs -i -t unzip {}; cd ..; done
----------    rm -rf  ./*/*.zip
------------------------------------------旧字典-------------------------------------------
drop table if exists extract.medicine_post_txt_add;
CREATE TABLE  if not exists extract.medicine_post_txt_add(
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

LOAD DATA LOCAL INPATH '/home/data/medicine/post/20170505/2006/*.txt'  INTO TABLE extract.medicine_post_txt_add;

-- 预处理
drop table if exists transforms.medicine_post_add;
CREATE TABLE  if not exists transforms.medicine_post_add(
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




insert into transforms.medicine_post_add PARTITION(platform_id)
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
from extract.medicine_post_txt_add
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





------------------------------------------新字典-------------------------------------------


drop table if exists extract.medicine_post_txt_add;
CREATE TABLE  if not exists extract.medicine_post_txt_add(
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

LOAD DATA LOCAL INPATH '/home/data/medicine/post/20170509/*/*.txt'  INTO TABLE extract.medicine_post_txt_add;



----去重
drop table if exists transforms.medicine_post_add_unique;
CREATE TABLE  if not exists transforms.medicine_post_add_unique(
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

insert into table transforms.medicine_post_add_unique
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
extract.medicine_post_txt_add m
) t where t.rn = 1
;




drop table if exists transforms.medicine_post_add;
CREATE TABLE  if not exists transforms.medicine_post_add(
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


insert into transforms.medicine_post_add PARTITION(platform_id)
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
transforms.medicine_post_add_unique t2
where t2.is_host = 1
;



insert into transforms.medicine_post_add PARTITION(platform_id)
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
t1.*, ROW_NUMBER() OVER ( Partition By post_id ORDER BY created_at asc) AS rn
FROM
transforms.medicine_post_add_unique t1
where t1.is_host != 1
) t2
;




------------------合并---------------------
insert into table l_medicine.post PARTITION(platform_id) select * from transforms.medicine_post_add;



-----------------------------------------------------------------水军-----------------------------------------------------------------------

