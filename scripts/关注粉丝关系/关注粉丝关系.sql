---------------------------------- 关注粉丝关系 ----------------------------------------------------
----------
----------
----------
----------

------------------------------------------------- 1、 数据规整 ----------------------------------------------------


drop table if exists transforms.follow;
CREATE TABLE  if not exists transforms.follow(
  platform_id   string
 ,user_id       string
 ,follow_id     string
 ,follow_count  string
 ,updated_at    string
 ,batch         string
)
CLUSTERED BY (user_id) INTO 113 BUCKETS
STORED AS orc
;


insert into table transforms.follow
select
  platform_id
 ,user_id
 ,follow_id
 ,follow_count
 ,updated_at
 ,case
      when batch =  1 then '2017-07-05'
      when batch =  2 then '2017-08-05'
  end
 from
 transforms.follow_bak
 ;


select count(1) from project.follow_tmp;
select count(1) from transforms.follow;





drop table if exists transforms.fans;
CREATE TABLE  if not exists transforms.fans(
 platform_id  string
,user_id      string
,fans_id      string
,fans_count   string
,updated_at   string
,batch        string
)
CLUSTERED BY (user_id) INTO 113 BUCKETS
STORED AS orc
;


insert into table transforms.fans
select
 platform_id
,user_id
,fans_id
,fans_count
,updated_at

,case
    when batch =  '第一批' then '2017-07-05'
    when batch =  '第二批' then '2017-08-05'
end

from
project.fans_tmp
;


select count(1) from project.fans_tmp;
select count(1) from transforms.fans;


------------------------------------------------- 2、 添加 关联关系    fans ----------------------------------------------------


drop table if exists project.fans_relation;
CREATE TABLE  if not exists project.fans_relation(
     platform_id      string
    ,source           string
    ,target           string
    ,current_status   string
    ,past_records     string
    ,updated_at       string
    ,batch            string
)
STORED AS orc
;


insert into table project.fans_relation
select
platform_id,
fans_id,
user_id,
'1',
concat(batch, ':', 1),
updated_at,
batch,
from
transforms.fans
where batch = '2017-07-05'
;




set batch=2017-08-05;

----- 2.1 插入 关注  数据
craete table project.fans_relation_new like project.fans_relation;
insert into table project.fans_relation_new
select
t1.platform_id,
t1.fans_id,
t1.user_id,
'1',
if(t2.source is null or t2.target is null, concat(t1.batch, ':', 1), concat(t2.past_records, '\;', t1.batch, ':', 1) ),
t1.updated_at,
t1.batch
from
(select * from transforms.fans where  batch = '${hiveconf:batch}' ) t1
left join
project.fans_relation t2
on t2.source = t1.fans_id and t2.target = t1.user_id
;

----- 2.2 插入 取消关注  数据
insert into table project.fans_relation_new
select
t1.platform_id,
t1.source,
t1.target,
'0',
concat(t1.past_records, '\;', '${hiveconf:batch}', ':', 0),
t1.updated_at,
'${hiveconf:batch}'
from
project.fans_relation t1
left join
(select * from transforms.fans where  batch = '${hiveconf:batch}' ) t2
on t1.source = t2.fans_id and t1.target = t2.user_id
where t2.user_id is null
;

use project;
drop table project.fans_relation;
alter table project.fans_relation_new rename to project.fans_relation;



------------------------------------------------- 3、 添加 关联关系    follow ----------------------------------------------------


drop table if exists project.follow_relation;
CREATE TABLE  if not exists project.follow_relation(
     platform_id  string
    ,source   string
    ,target   string
    ,current_status   string
    ,past_records   string
    ,updated_at   string
    ,batch string
)
STORED AS orc
;


insert into table project.follow_relation
select
platform_id,
user_id,
follow_id,
'1',
concat(batch, ':', 1),
updated_at,
batch
from
transforms.follow
where batch = '2017-07-05'
;



set batch=2017-08-05;

----- 3.1 插入 关注  数据


create table project.follow_relation_new like project.follow_relation;
insert into table project.follow_relation_new
select
t1.platform_id,
t1.user_id,
t1.follow_id,
'1',
if(t2.source is null or t2.target is null, concat(t1.batch, ':', 1), concat(t2.past_records, '\;', t1.batch, ':', 1) ),
t1.updated_at,
t1.batch
from
(select * from transforms.follow where  batch = '${hiveconf:batch}' ) t1
left join
project.follow_relation t2
on t2.source = t1.user_id and t2.target = t1.follow_id
;


----- 3.2 插入 取消关注  数据
insert into table project.follow_relation_new
select
t1.platform_id,
t1.source,
t1.target,
'0',
concat(t1.past_records, '\;', '${hiveconf:batch}', ':', 0),
t1.updated_at,
'${hiveconf:batch}'
from
project.follow_relation t1
left join
(select * from transforms.follow where  batch = '${hiveconf:batch}' ) t2
on t1.source = t2.user_id and t1.target = t2.follow_id
where t2.user_id is null
;

use project;
drop table project.follow_relation;
alter table project.follow_relation_new rename to project.follow_relation;


