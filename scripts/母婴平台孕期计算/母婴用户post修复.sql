
-----------------------------------------user 去重及其 计算 孕期-------------------------------------------------------
---- 1、 计算用户表中 的birthday
drop table if exists transforms.user_birthday_1;
create table transforms.user_birthday_1 like l_motherbaby.user;
add file /home/script/test/mpintranet/gestation_compute_user_9.py;


set mapred.reduce.tasks = 200;

insert into table transforms.user_birthday_1 PARTITION(platform_id)
select
TRANSFORM (
     user_id
    ,brief_intro
    ,user_tags
    ,user_name
    ,detail_url
    ,user_gender
    ,user_birthday
    ,user_age
    ,user_level
    ,baby_count
    ,baby_info
    ,baby_gender
    ,if(platform_id = 1007, to_date(substr(baby_info,instr(baby_info,"Birthday")+12,10)), baby_birthday)
    ,baby_agenow
    ,ask_count
    ,reply_count
    ,post_count
    ,reply_post_count
    ,quality_post_count
    ,best_answer_count
    ,fans_count
    ,following_count
    ,device
    ,address
    ,tel
    ,province
    ,city
    ,created_at
    ,updated_at
    ,noise
    ,platform_id
)
USING "python gestation_compute_user_9.py"
AS (
     user_id
    ,brief_intro
    ,user_tags
    ,user_name
    ,detail_url
    ,user_gender
    ,user_birthday
    ,user_age
    ,user_level
    ,baby_count
    ,baby_info
    ,baby_gender
    ,baby_birthday
    ,baby_agenow
    ,ask_count
    ,reply_count
    ,post_count
    ,reply_post_count
    ,quality_post_count
    ,best_answer_count
    ,fans_count
    ,following_count
    ,device
    ,address
    ,tel
    ,province
    ,city
    ,created_at
    ,updated_at
    ,noise
    ,platform_id
)

from l_motherbaby.user;


----- 1.2  user_birthday_1 去重
drop table if exists l_motherbaby.user_birthday;
create table l_motherbaby.user_birthday like l_motherbaby.user;

insert into l_motherbaby.user_birthday PARTITION(platform_id)
select
 t2.user_id
,t2.brief_intro
,t2.user_tags
,t2.user_name
,t2.detail_url
,t2.user_gender
,t2.user_birthday
,t2.user_age
,t2.user_level
,t2.baby_count
,t2.baby_info
,t2.baby_gender
,t2.baby_birthday
,t2.baby_agenow
,t2.ask_count
,t2.reply_count
,t2.post_count
,t2.reply_post_count
,t2.quality_post_count
,t2.best_answer_count
,t2.fans_count
,t2.following_count
,t2.device
,t2.address
,t2.tel
,t2.province
,t2.city
,t2.created_at
,t2.updated_at
,t2.noise
,t2.platform_id
from
(
SELECT
t1.*, ROW_NUMBER() OVER ( Partition By platform_id, user_id ORDER BY created_at asc) AS rn
FROM
transforms.user_birthday_1 t1
) t2 where t2.rn = 1
;




-----------------------------------------post 去重-------------------------------------------------------


drop table if exists transforms.post_unique;
create table transforms.post_unique like l_motherbaby.post;
set mapred.reduce.tasks = 5000;

insert into transforms.post_unique PARTITION(platform_id)
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
,t2.baby_days
,t2.floorid
,t2.noise
,t2.platform_id
from
(
SELECT
t1.*, ROW_NUMBER() OVER ( Partition By t1.platform_id, t1.id ORDER BY t1.created_at desc) AS rn
FROM
l_motherbaby.post t1
) t2 where t2.rn = 1
;


-----------------------------------------post 计算 孕期-------------------------------------------------------

------2、 修复post 表中的subject

set mapred.reduce.tasks = 3000;


drop table if exists l_motherbaby.post_subject;
add file /home/script/test/mpintranet/gestation_compute_post_14.py;

drop table if exists l_motherbaby.post_subject;
CREATE TABLE  if not exists l_motherbaby.post_subject(
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


insert into l_motherbaby.post_subject PARTITION(platform_id)
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
    ,t2.baby_days
    ,t2.floorid
    ,t2.noise
    ,t2.platform_id
)
USING "python gestation_compute_post_14.py"
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
transforms.post_unique t2
;

-----------------------------------------post user join 孕期-------------------------------------------------------


------3、 修复post中的 babydays
drop table if exists l_motherbaby.post_join_user;
CREATE TABLE  if not exists l_motherbaby.post_join_user(
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
STORED AS orc;



set mapred.reduce.tasks = 3000;

insert into l_motherbaby.post_join_user PARTITION(platform_id)
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
,t1.baby_days
,t1.floorid
,t1.noise
,t1.platform_id
from
l_motherbaby.post_subject t1
WHERE
nvl(user_id, '') in ('', '1005:#')
;




insert into l_motherbaby.post_join_user PARTITION(platform_id)
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
(select * from l_motherbaby.post_subject where nvl(user_id, '') != '' and platform_id in (1001, 1003) ) t1
left join
(select * from l_motherbaby.user_birthday where platform_id in (1001, 1003) ) t2
on t1.user_id = t2.user_id and t1.platform_id = t2.platform_id
;




insert into l_motherbaby.post_join_user PARTITION(platform_id)
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
(select * from l_motherbaby.post_subject where nvl(user_id, '') not in ('', '1005:#') and platform_id in (1004, 1005) ) t1
left join
(select * from l_motherbaby.user_birthday where platform_id in (1004, 1005) ) t2
on t1.user_id = t2.user_id and t1.platform_id = t2.platform_id
;



insert into l_motherbaby.post_join_user PARTITION(platform_id)
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
(select * from l_motherbaby.post_subject where nvl(user_id, '') != '' and platform_id in (1006, 1007, 1008) ) t1
left join
(select * from l_motherbaby.user_birthday where platform_id in (1006, 1007, 1008) ) t2
on t1.user_id = t2.user_id and t1.platform_id = t2.platform_id
;



insert into l_motherbaby.post_join_user PARTITION(platform_id)
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
(select * from l_motherbaby.post_subject where nvl(user_id, '') != '' and platform_id in (1013, 1015, 1016, 1102, 1104) ) t1
left join
(select * from l_motherbaby.user_birthday where platform_id in (1013, 1015, 1016, 1102, 1104) ) t2
on t1.user_id = t2.user_id and t1.platform_id = t2.platform_id
;




insert into l_motherbaby.post_join_user PARTITION(platform_id)
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
(select * from l_motherbaby.post_subject where nvl(user_id, '') != '' and platform_id in (1105, 1106, 1108, 1110, 1113, 1114) ) t1
left join
(select * from l_motherbaby.user_birthday where platform_id in (1105, 1106, 1108, 1110, 1113, 1114) ) t2
on t1.user_id = t2.user_id and t1.platform_id = t2.platform_id
;

-----------------------------------------测试 数据可读写 性   重点 1005-------------------------------------------------------



-- (1001, 1003)
-- (1004, 1005)
-- (1006, 1007, 1008)
-- (1013, 1015, 1016, 1102, 1104)
-- (1105, 1106, 1108, 1110, 1113, 1114)






