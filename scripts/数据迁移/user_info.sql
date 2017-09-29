--------------------------------------------------------------------------------------
----------                       weibo 、医药、 母婴, user 表迁移
---------- 1、医药用户表迁移

---------- 2、母婴用户表迁移
----------
--------------------------------------------------------------------------------------


---------- 1、医药用户表迁移

--- user_doc
insert overwrite local directory "/home/data/tmp/user_doc_distinct"
row format delimited
fields terminated by "\t"
select * from mp_hcp.user_doc_distinct;


drop table if exists extract.medicine_user_doc_txt;
CREATE TABLE  if not exists extract.medicine_user_doc_txt(
userid               string,
userprofileurl       string,
username             string,
age                  string,
gender               string,
hospital             string,
hospitalurl          string,
department           string,
section              string,
jobtitle             string,
academictitle        string,
hopitalprovince      string,
hopitalcity          string,
acceptedrate         string,
replycount           string,
likescount           string,
satisfyratio         string,
bestanswercount      string,
helpedusercount      string,
userthankcount       string,
fanscount            string,
diagcount            string,
contributions        string,
totalvisitcount      string,
totalpatientcount    string,
thankslettercount    string,
gifts                string,
lastonlinedate       string,
registereddate       string,
ratingscore          string,
specility            string,
introduction         string,
crawldate            string,
platform             string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY'\t';

LOAD DATA LOCAL INPATH '/home/data/medicine/medicine_user_doc.csv'  INTO TABLE extract.medicine_user_doc_txt;



----- new_ dict

drop table if exists transforms.medicine_user_doc;
CREATE TABLE  if not exists transforms.medicine_user_doc(
user_id string ,
MPDoctorID string ,
user_name string ,
detail_url string ,
brief_intro string ,
user_speciality string ,
user_hospital string ,
user_hospital_url string ,
user_department string ,
user_job_title string ,
user_academic_title string ,
Address string ,
Tel string ,
Province string ,
City string ,
user_birthday string ,
user_age int ,
user_gender string ,
accepted_rate string ,
reply_count int ,
like_count int ,
satisfid_rate string ,
best_answer_count int ,
helped_user_count int ,
user_thank_count int ,
fans_count int ,
diag_count int ,
contribution_value int ,
total_visit_count int ,
total_patient_count int ,
thanks_letter_count int ,
gift_count int ,
last_onlined_at string ,
created_at string ,
rating_score string ,
updated_at string ,
noise int
)
PARTITIONED BY (platform_id string)
STORED AS orc;


insert into transforms.medicine_user_doc PARTITION(platform_id)
select
if(nvl(userid, '') != '', concat(platform, ":", userid), userid)
,''
,username
,userprofileurl
,introduction
,specility
,hospital
,hospitalurl
,department
,jobtitle
,academictitle
,''
,''
,hopitalprovince
,hopitalcity
,''
,age
,gender
,acceptedrate
,replycount
,likescount
,satisfyratio
,bestanswercount
,helpedusercount
,userthankcount
,fanscount
,diagcount
,contributions
,totalvisitcount
,totalpatientcount
,thankslettercount
,gifts
,lastonlinedate
,registereddate
,ratingscore
,crawldate
,''
,platform
from
extract.medicine_user_doc_txt
where  length(nvl(platform, '')) = 4
;


--- platform 和 crawldate 位置相反

insert into transforms.medicine_user_doc PARTITION(platform_id)
select
 if(nvl(userid, '') != '', concat(platform, ":", userid), userid)
,''
,username
,userprofileurl
,introduction
,specility
,hospital
,hospitalurl
,department
,jobtitle
,academictitle
,''
,''
,hopitalprovince
,hopitalcity
,''
,age
,gender
,acceptedrate
,replycount
,likescount
,satisfyratio
,bestanswercount
,helpedusercount
,userthankcount
,fanscount
,diagcount
,contributions
,totalvisitcount
,totalpatientcount
,thankslettercount
,gifts
,lastonlinedate
,registereddate
,ratingscore
,platform
,''
,crawldate
from
extract.medicine_user_doc_txt
where  length(nvl(crawldate, '')) = 4
;




--- user

insert overwrite local directory "/home/data/tmp/user"
row format delimited
fields terminated by "\t"
select * from mp_hcp.user;


drop table if exists extract.medicine_user_txt;
CREATE TABLE  if not exists extract.medicine_user_txt(
userid                   string,
userprofileurl           string,
username                 string,
age                      string,
gender                   string,
level                    string,
city                     string,
province                 string,
askcount                 string,
replycount               string,
postcount                string,
replypostcount           string,
qualitypostcount         string,
bestanswercount          string,
fanscount                string,
followingcount           string,
babycount                string,
babyinfo                 string,
registerdate             string,
crawldate                string,
platform                 string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY'\t';


LOAD DATA LOCAL INPATH '/home/data/medicine/medicine_user.csv'  INTO TABLE extract.medicine_user_txt;



-------------------------2006 平台处理--------------

drop table if exists extract.medicine_user_txt;
CREATE TABLE  if not exists extract.medicine_user_txt(
 UserID string
,UserProfileUrl string
,UserName string
,Age string
,Gender string
,Hospital string
,HospitalUrl string
,Department string
,ection string
,JobTitle string
,AcademicTitle string
,HopitalProvince string
,HopitalCity string
,ReplyCount string
,SatisfyRatio string
,BestAnswerCount string
,HelpedUserCount string
,UserThankCount string
,FansCount string
,Contributions string
,TotalVisitCount string
,TotalPatientCount string
,ThanksLetterCount string
,Gifts string
,LastOnlineDate string
,RegisteredDate string
,RatingScore string
,Specility string
,Introduction string
,Platform string
,CrawlDate string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY'\t';





LOAD DATA LOCAL INPATH '/home/data/medicine/user_2006.csv'  INTO TABLE extract.medicine_user_txt;


-----------new dict------------------

drop table if exists transforms.medicine_user;
CREATE TABLE  if not exists transforms.medicine_user(
user_id string,
brief_intro string,
user_tags string,
user_name string,
detail_url string,
user_gender string,
user_birthday string,
user_age string,
user_level string,
Baby_Count string,
baby_info string,
baby_gender string,
baby_birthday string,
baby_agenow string,
Ask_Count string,
Reply_Count string,
Post_Count string,
Reply_Post_Count string,
Quality_Post_Count string,
Best_Answer_Count string,
Fans_Count string,
Following_Count string,
device string,
Address string,
Tel string,
Province string,
City string,
created_at string,
updated_at string,
noise int
)
PARTITIONED BY (Platform_ID string)
STORED AS orc;



insert into transforms.medicine_user PARTITION(Platform_ID)
select
if(nvl(userid, '') != '', concat(platform, ":", userid), userid)
,''
,''
,username
,userprofileurl
,gender
,''
,age
,level
,babycount
,babyinfo
,''
,''
,''
,askcount
,replycount
,postcount
,replypostcount

,qualitypostcount
,bestanswercount
,fanscount
,followingcount
,''
,''
,''
,province
,city
,registerdate
,crawldate
,''
,platform
from
medicine.user_txt
where
length(nvl(platform, '')) = 4
and nvl(userid, '') != ''
and nvl(crawldate, '') != ''
;



--------2006 平台处理-------

insert into transforms.medicine_user PARTITION(Platform_ID)
select
if(nvl(userid, '') != '', concat(platform, ":", userid), userid)
,Introduction
,''
,UserName
,UserProfileUrl
,Gender
,''
,Age
,''
,''
,''
,''
,''
,''
,''
,ReplyCount
,''
,''
,''
,''
,FansCount
,''
,''
,''
,''
,HopitalProvince
,HopitalCity
,RegisteredDate
,CrawlDate
,''
,platform
from
extract.medicine_user_txt
where
length(nvl(platform, '')) = 4
and nvl(userid, '') != ''
and nvl(crawldate, '') != ''
;


---------- 2、母婴用户表迁移

-- motherbaby_user

insert overwrite local directory "/home/data/tmp/user_distinct"
row format delimited
fields terminated by "\t"
select * from health_mother_inquiry.user_distinct;


drop table if exists extract.motherbaby_user_txt;
CREATE TABLE  if not exists extract.motherbaby_user_txt(
userid                  string,
userprofileurl          string,
username                string,
age                     string,
gender                  string,
level                   string,
city                    string,
province                string,
askcount                string,
replycount              string,
postcount               string,
replypostcount          string,
qualitypostcount        string,
bestanswercount         string,
fanscount               string,
followingcount          string,
babycount               string,
babyinfo                string,
registerdate            string,
crawldate               string,
platform                string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY'\t';


LOAD DATA LOCAL INPATH '/home/data/motherbaby/motherbaby_user.csv'  INTO TABLE extract.motherbaby_user_txt;




drop table if exists transforms.motherbaby_user;
CREATE TABLE  if not exists transforms.motherbaby_user(
user_id string,
brief_intro string,
user_tags string,
user_name string,
detail_url string,
user_gender string,
user_birthday string,
user_age int,
user_level string,
Baby_Count int,
baby_info string,
baby_gender string,
baby_birthday string,
baby_agenow string,
Ask_Count int,
Reply_Count int,
Post_Count int,
Reply_Post_Count int,
Quality_Post_Count int,
Best_Answer_Count int,
Fans_Count int,
Following_Count int,
device string,
Address string,
Tel string,
Province string,
City string,
created_at string,
updated_at string,
noise int
)
PARTITIONED BY (Platform_ID string)
STORED AS orc;



insert into transforms.motherbaby_user PARTITION(Platform_ID)
select
if(nvl(userid, '') != '', concat(platform, ":", userid), userid)
,''
,''
,username
,userprofileurl
,gender
,''
,age
,level
,babycount
,babyinfo
,''
,''
,''
,askcount
,replycount
,postcount
,replypostcount
,qualitypostcount
,bestanswercount
,fanscount
,followingcount
,''
,''
,''
,province
,city
,registerdate
,crawldate
,''
,platform
from
motherbaby.user_txt
where
length(nvl(platform, '')) = 4
and nvl(userid, '') != ''
and nvl(crawldate, '') != ''
;





drop table if exists transforms.motherbaby_user_error;
CREATE TABLE  if not exists transforms.motherbaby_user_error
STORED AS orc
as
select
*
from extract.motherbaby_user_txt
where
not (
length(nvl(platform, '')) = 4
and nvl(userid, '') != ''
and nvl(crawldate, '') != ''
)
;








--------------------------------------------------------迁移至l----------------------------------------------------------------------

drop table if exists l_medicine.user;
create table l_medicine.user like transforms.medicine_user;
insert into table l_medicine.user PARTITION(Platform_ID)
select
*
from
transforms.medicine_user
;


drop table if exists l_medicine.user_doc;
create table l_medicine.user_doc like transforms.medicine_user_doc;
insert into table l_medicine.user_doc PARTITION(Platform_ID)
select
*
from
transforms.medicine_user_doc
;



drop table if exists l_motherbaby.user;
create table l_motherbaby.user like transforms.motherbaby_user;
insert into table l_motherbaby.user PARTITION(Platform_ID)
select
*
from
transforms.motherbaby_user
;




---------------------------------------------------------新字典用户入库--------------------------------------------------------------------


----------母婴用户入库---------------
drop table if exists extract.motherbaby_user_txt_add;
CREATE TABLE  if not exists extract.motherbaby_user_txt_add(
   platform_id          int
  ,user_id              string
  ,brief_intro          string
  ,user_tags            string
  ,user_name            string
  ,detail_url           string
  ,user_gender          string
  ,user_birthday        string
  ,user_age             string
  ,user_level           string
  ,baby_count           string
  ,baby_info            string
  ,baby_gender          string
  ,baby_birthday        string
  ,baby_agenow          string
  ,ask_count            string
  ,reply_count          string
  ,post_count           string
  ,reply_post_count     string
  ,quality_post_count   string
  ,best_answer_count    string
  ,fans_count           string
  ,following_count      string
  ,device               string
  ,address              string
  ,tel                  string
  ,province             string
  ,city                 string
  ,created_at           string
  ,updated_at           string
  ,noise                string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY'\t';


LOAD DATA LOCAL INPATH '/home/data/motherbaby_user/${hivevar:date_range}/*/*.txt'  INTO TABLE extract.motherbaby_user_txt_add;



add file /home/script/test/mpintranet/gestation_compute_user_10.py;

insert into table l_motherbaby.user PARTITION(platform_id)
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
USING "python gestation_compute_user_10.py"
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

from extract.motherbaby_user_txt_add;




----- --------------------   1.2  user_birthday_1 去重   ------------------------------
drop table if exists l_motherbaby.user_unique;
create table l_motherbaby.user_unique like l_motherbaby.user;

insert into l_motherbaby.user_unique PARTITION(platform_id)
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
l_motherbaby.user t1
) t2 where t2.rn = 1
;

drop table if exists l_motherbaby.user_bak;
user l_motherbaby;
alter table l_motherbaby.user rename to l_motherbaby.user_bak;
alter table l_motherbaby.user_unique rename to l_motherbaby.user;













-----------------医药用户入库-------------------
drop table if exists extract.medicine_user_txt_add;
CREATE TABLE  if not exists extract.medicine_user_txt_add(
   platform_id          int
  ,user_id              string
  ,brief_intro          string
  ,user_tags            string
  ,user_name            string
  ,detail_url           string
  ,user_gender          string
  ,user_birthday        string
  ,user_age             string
  ,user_level           string
  ,baby_count           string
  ,baby_info            string
  ,baby_gender          string
  ,baby_birthday        string
  ,baby_agenow          string
  ,ask_count            string
  ,reply_count          string
  ,post_count           string
  ,reply_post_count     string
  ,quality_post_count   string
  ,best_answer_count    string
  ,fans_count           string
  ,following_count      string
  ,device               string
  ,address              string
  ,tel                  string
  ,province             string
  ,city                 string
  ,created_at           string
  ,updated_at           string
  ,noise                string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY'\t';


LOAD DATA LOCAL INPATH '/home/data/medicine_user/user/20170509/*/*.txt'  INTO TABLE extract.medicine_user_txt_add;



insert into table l_medicine.user PARTITION(Platform_ID)
select
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
from
extract.medicine_user_txt_add
;






drop table if exists extract.medicine_user_doc_txt_add;
CREATE TABLE  if not exists extract.medicine_user_doc_txt_add(
 platform_id           string  ,
 user_id               string  ,
 mpdoctorid            string  ,
 user_name             string  ,
 detail_url            string  ,
 brief_intro           string  ,
 user_speciality       string  ,
 user_hospital         string  ,
 user_hospital_url     string  ,
 user_department       string  ,
 user_job_title        string  ,
 user_academic_title   string  ,
 address               string  ,
 tel                   string  ,
 province              string  ,
 city                  string  ,
 user_birthday         string  ,
 user_age              string  ,
 user_gender           string  ,
 accepted_rate         string  ,
 reply_count           string  ,
 like_count            string  ,
 satisfid_rate         string  ,
 best_answer_count     string  ,
 helped_user_count     string  ,
 user_thank_count      string  ,
 fans_count            string  ,
 diag_count            string  ,
 contribution_value    string  ,
 total_visit_count     string  ,
 total_patient_count   string  ,
 thanks_letter_count   string  ,
 gift_count            string  ,
 last_onlined_at       string  ,
 created_at            string  ,
 rating_score          string  ,
 updated_at            string  ,
 noise                 string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY'\t';


LOAD DATA LOCAL INPATH '/home/data/medicine_user/user/20170523/*/*.txt'  INTO TABLE extract.medicine_user_doc_txt_add;



insert into table l_medicine.user_doc PARTITION(Platform_ID)
select
 user_id               ,
 mpdoctorid            ,
 user_name             ,
 detail_url            ,
 brief_intro           ,
 user_speciality       ,
 user_hospital         ,
 user_hospital_url     ,
 user_department       ,
 user_job_title        ,
 user_academic_title   ,
 address               ,
 tel                   ,
 province              ,
 city                  ,
 user_birthday         ,
 user_age              ,
 user_gender           ,
 accepted_rate         ,
 reply_count           ,
 like_count            ,
 satisfid_rate         ,
 best_answer_count     ,
 helped_user_count     ,
 user_thank_count      ,
 fans_count            ,
 diag_count            ,
 contribution_value    ,
 total_visit_count     ,
 total_patient_count   ,
 thanks_letter_count   ,
 gift_count            ,
 last_onlined_at       ,
 created_at            ,
 rating_score          ,
 updated_at            ,
 noise                 ,
 platform_id
from
extract.medicine_user_doc_txt_add
;