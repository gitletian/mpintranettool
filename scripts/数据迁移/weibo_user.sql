--------------------------------------------------------------------------------------
----------                       weibo user 自动化
---------- 1、weibo
----------
--------------------------------------------------------------------------------------


drop table if exists extract.socialmedia_weibo_user_txt;
CREATE TABLE  if not exists extract.socialmedia_weibo_user_txt(
platform_id string,
user_id string,
user_name string,
user_real_name string,
user_location string,
user_gender string,
user_sex_orientation string,
user_relationship_status string,
user_birthday string,
user_level string,
blood_type string,
blog_url string,
weibo_personal_url string,
brief_intro string,
email string,
qq string,
msn string,
job_info string,
edu_info string,
tags string,
following_count string,
fans_count string,
blog_count string,
verified_intro string,
verified_type string,
detail_url string,
created_at string,
updated_at string,
noise int
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY'\t';

LOAD DATA LOCAL INPATH '/home/data/socialmedia/WeiboUser.txt'  INTO TABLE extract.socialmedia_weibo_user_txt;




drop table if exists transforms.socialmedia_weibo_user;
CREATE TABLE  if not exists transforms.socialmedia_weibo_user(
platform_id string,
user_id string,
user_name string,
user_real_name string,
user_location string,
user_gender string,
user_sex_orientation string,
user_relationship_status string,
user_birthday string,
user_level int,
blood_type string,
blog_url string,
weibo_personal_url string,
brief_intro string,
email string,
qq string,
msn string,
job_info string,
edu_info string,
tags string,
following_count int,
fans_count int,
blog_count int,
verified_intro string,
verified_type string,
detail_url string,
created_at string,
updated_at string,
noise int
)
STORED AS orc;

insert into table transforms.socialmedia_weibo_user
select
 platform_id
,user_id
,user_name
,user_real_name
,user_location
,user_gender
,user_sex_orientation
,user_relationship_status
,user_birthday
,user_level
,blood_type
,blog_url
,weibo_personal_url
,brief_intro
,email
,qq
,msn
,job_info
,edu_info
,tags
,following_count
,fans_count
,blog_count
,verified_intro
,verified_type
,detail_url
,concat(created_at, ' 00:00:00')
,concat(updated_at, ' 00:00:00')
,noise

from extract.socialmedia_weibo_user_txt
;
