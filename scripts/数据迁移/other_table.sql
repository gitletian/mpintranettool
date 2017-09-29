--------------------------------------------------------------------------------------
----------                       其余表的迁移
----------  1、第一阶段
----------  2、第二阶段
--------------------------------------------------------------------------------------

--------------------1、第一阶段
----1、

drop table if exists das.das_testdata_20170224_txt;
CREATE TABLE  if not exists das.das_testdata_20170224_txt(
id                  string   ,
example_a          string   ,
example_b                string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY'\t';

LOAD DATA LOCAL INPATH '/mnt/disk6/das/das_testdata_20170224.csv'  INTO TABLE das.das_testdata_20170224_txt;

drop table if exists das.das_testdata_20170224;
CREATE TABLE  if not exists das.das_testdata_20170224(
id                  string   ,
example_a          string   ,
example_b                string
)
STORED AS ORC;

insert into table das.das_testdata_20170224 select * from das.das_testdata_20170224_txt;

----2、

drop table if exists das.lsk_6plat_0120_txt;
CREATE TABLE  if not exists das.lsk_6plat_0120_txt(
id             string,
url            string,
postid         string,
floorid        string,
title          string,
content        string,
userid         string,
usertype       string,
username       string,
userprofileurl string,
postdate       string,
month          string,
hospital       string,
department     string,
jobtitle       string,
platform       string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY'\t';

LOAD DATA LOCAL INPATH '/mnt/disk6/das/lsk_6plat_0120.csv'  INTO TABLE das.lsk_6plat_0120_txt;

drop table if exists das.lsk_6plat_0120;
CREATE TABLE  if not exists das.lsk_6plat_0120(
id             string,
url            string,
postid         string,
floorid        string,
title          string,
content        string,
userid         string,
usertype       string,
username       string,
userprofileurl string,
postdate       string,
month          string,
hospital       string,
department     string,
jobtitle       string,
platform       string
)
CLUSTERED BY (id) INTO 113 BUCKETS
STORED AS ORC;

insert into table das.lsk_6plat_0120 select * from das.lsk_6plat_0120_txt;


----3、 ？
drop table if exists das.lsk_6plat_0120_attr_txt;
CREATE TABLE  if not exists das.lsk_6plat_0120_attr_txt(
id             string,
attr            string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY'\t';

LOAD DATA LOCAL INPATH '/mnt/disk6/das/lsk_6plat_0120_attr.csv'  INTO TABLE das.lsk_6plat_0120_attr_txt;

drop table if exists das.lsk_6plat_0120_attr;
CREATE TABLE  if not exists das.lsk_6plat_0120_attr(
id             string,
attr            string
)
STORED AS ORC;

insert into table das.lsk_6plat_0120_attr select * from das.lsk_6plat_0120_attr_txt;
	4、
drop table if exists das.lsk_6plat_0221_txt;
CREATE TABLE  if not exists das.lsk_6plat_0221_txt(
id                string,
url               string,
postid            string,
floorid           string,
title             string,
content           string,
userid            string,
usertype          string,
username          string,
userprofileurl    string,
postdate          string,
month             string,
hospital          string,
department        string,
jobtitle          string,
platform          string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY'\t';

LOAD DATA LOCAL INPATH '/mnt/disk6/das/lsk_6plat_0221.csv'  INTO TABLE das.lsk_6plat_0221_txt;

insert into table das.lsk_6plat_0221 select * from das.lsk_6plat_0221_txt;

	5、
drop table if exists das.lsk_weibo_cleaned_0221_txt;
CREATE TABLE  if not exists das.lsk_weibo_cleaned_0221_txt(
id                   string,
wid                  string,
text                 string,
source               string,
geo                  string,
pic_urls             string,
uid                  string,
province             string,
city                 string,
ulocation            string,
ugender              string,
ufollowers_count     string,
ufriends_count       string,
ustatuses_count      string,
ufavourites_count    string,
uverified            string,
verified_reason      string,
ubi_followers_count  string,
created_at           string,
water                string,
month                string,
date                 string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY'\t';

LOAD DATA LOCAL INPATH '/mnt/disk6/das/lsk_weibo_cleaned_0221.csv'  INTO TABLE das.lsk_weibo_cleaned_0221_txt;

drop table if exists das.lsk_weibo_cleaned_0221;
CREATE TABLE  if not exists das.lsk_weibo_cleaned_0221(
id                   string,
wid                  string,
text                 string,
source               string,
geo                  string,
pic_urls             string,
uid                  string,
province             string,
city                 string,
ulocation            string,
ugender              string,
ufollowers_count     string,
ufriends_count       string,
ustatuses_count      string,
ufavourites_count    string,
uverified            string,
verified_reason      string,
ubi_followers_count  string,
created_at           string,
water                string,
month                string,
date                 string
)
STORED AS ORC;

insert into table das.lsk_weibo_cleaned_0221 select * from das.lsk_weibo_cleaned_0221_txt;

	6、
drop table if exists das.lsk_weibo_cleaned_0221_attr_txt;
CREATE TABLE  if not exists das.lsk_weibo_cleaned_0221_attr_txt(
id             string,
attr            string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY'\t';

LOAD DATA LOCAL INPATH '/mnt/disk6/das/lsk_weibo_cleaned_0221_attr.csv'  INTO TABLE das.lsk_weibo_cleaned_0221_attr_txt;

drop table if exists das.lsk_weibo_cleaned_0221_attr;
CREATE TABLE  if not exists das.lsk_weibo_cleaned_0221_attr(
id             string,
attr            string
)
STORED AS ORC;

insert into table das.lsk_weibo_cleaned_0221_attr select * from das.lsk_weibo_cleaned_0221_attr_txt;

	7、
drop table if exists das.priorpregnancy_inquiry_2016_txt;
CREATE TABLE  if not exists das.priorpregnancy_inquiry_2016_txt(
id                   string,
channel              string,
subject              string,
contenttype          string,
isbestanswer         string,
ishost               string,
url                  string,
postid               string,
floorid              string,
title                string,
content              string,
tags                 string,
userid               string,
usertype             string,
username             string,
userprofileurl       string,
gender               string,
birthday             string,
userlevel            string,
location             string,
babybirthday         string,
babyagethen          string,
postdate             string,
userstate            string,
replycount           string,
viewcount            string,
collectioncount      string,
device               string,
hospital             string,
department           string,
section              string,
jobtitle             string,
academictitle        string,
speciality           string,
likes                string,
crawldate            string,
platform             string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY'\t';

LOAD DATA LOCAL INPATH '/mnt/disk6/das/priorpregnancy_inquiry_2016.csv'  INTO TABLE das.priorpregnancy_inquiry_2016_txt;

drop table if exists das.priorpregnancy_inquiry_2016;
CREATE TABLE  if not exists das.priorpregnancy_inquiry_2016(
id                   string,
channel              string,
subject              string,
contenttype          string,
isbestanswer         string,
ishost               string,
url                  string,
postid               string,
floorid              string,
title                string,
content              string,
tags                 string,
userid               string,
usertype             string,
username             string,
userprofileurl       string,
gender               string,
birthday             string,
userlevel            string,
location             string,
babybirthday         string,
babyagethen          string,
postdate             string,
userstate            string,
replycount           string,
viewcount            string,
collectioncount      string,
device               string,
hospital             string,
department           string,
section              string,
jobtitle             string,
academictitle        string,
speciality           string,
likes                string,
crawldate            string,
platform             string
)
STORED AS ORC;

insert into table das.priorpregnancy_inquiry_2016 select * from das.priorpregnancy_inquiry_2016_txt;

	8、
drop table if exists das.priorpregnancy_inquiry_2016_attr_txt;
CREATE TABLE  if not exists das.priorpregnancy_inquiry_2016_attr_txt(
id             string,
attr            string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY'\t';

LOAD DATA LOCAL INPATH '/mnt/disk6/das/priorpregnancy_inquiry_2016_attr.csv'  INTO TABLE das.priorpregnancy_inquiry_2016_attr_txt;

drop table if exists das.priorpregnancy_inquiry_2016_attr;
CREATE TABLE  if not exists das.priorpregnancy_inquiry_2016_attr(
id             string,
attr            string
)
STORED AS ORC;

insert into table das.priorpregnancy_inquiry_2016_attr select * from das.priorpregnancy_inquiry_2016_attr_txt;

	9、
drop table if exists medicine.mp_department_txt;
CREATE TABLE  if not exists medicine.mp_department_txt(
mpdepartmentid      string,
departmentname      string,
departmentlevel     string,
level1              string,
level2              string,
level3              string,
medicalsystem       string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY'\t';

LOAD DATA LOCAL INPATH '/mnt/disk6/das/mp_department.csv'  INTO TABLE medicine.mp_department_txt;

drop table if exists medicine.mp_department;
CREATE TABLE  if not exists medicine.mp_department(
mpdepartmentid      string,
departmentname      string,
departmentlevel     string,
level1              string,
level2              string,
level3              string,
medicalsystem       string
)
STORED AS ORC;

insert into table medicine.mp_department select * from medicine.mp_department_txt;

	10、
drop table if exists medicine.mp_department_map_txt;
CREATE TABLE  if not exists medicine.mp_department_map_txt(
departmentalias      string,
mpdepartmentid      string,
departmentname     string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY'\t';

LOAD DATA LOCAL INPATH '/mnt/disk6/das/mp_department_map.csv'  INTO TABLE medicine.mp_department_map_txt;

drop table if exists medicine.mp_department_map;
CREATE TABLE  if not exists medicine.mp_department_map(
departmentalias      string,
mpdepartmentid      string,
departmentname     string
)
STORED AS ORC;

insert into table medicine.mp_department_map select * from medicine.mp_department_map_txt;

	11、
drop table if exists medicine.mp_jobtitle_txt;
CREATE TABLE  if not exists medicine.mp_jobtitle_txt(
mptitleid                string,
titlename                string,
vocationalqualification  string,
medicalquality           string,
level                    string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY'\t';

LOAD DATA LOCAL INPATH '/mnt/disk6/das/mp_jobtitle.csv'  INTO TABLE medicine.mp_jobtitle_txt;
drop table if exists medicine.mp_jobtitle;
CREATE TABLE  if not exists medicine.mp_jobtitle(
mptitleid                string,
titlename                string,
vocationalqualification  string,
medicalquality           string,
level                    string
)
STORED AS ORC;

insert into table medicine.mp_jobtitle select * from medicine.mp_jobtitle_txt;

	12、
drop table if exists medicine.mp_jobtitle_map_txt;
CREATE TABLE  if not exists medicine.mp_jobtitle_map_txt(
titlealias                string,
mptitleid                 string,
titlename                 string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY'\t';

LOAD DATA LOCAL INPATH '/mnt/disk6/das/mp_jobtitle_map.csv'  INTO TABLE medicine.mp_jobtitle_map_txt;

drop table if exists medicine.mp_jobtitle_map;
CREATE TABLE  if not exists medicine.mp_jobtitle_map(
titlealias                string,
mptitleid                 string,
titlename                 string
)
STORED AS ORC;

insert into table medicine.mp_jobtitle_map select * from medicine.mp_jobtitle_map_txt;

	13、
drop table if exists medicine.mp_level_map_txt;
CREATE TABLE  if not exists medicine.mp_level_map_txt(
levelalias                string,
mp_hoslevel               string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY'\t';

LOAD DATA LOCAL INPATH '/mnt/disk6/das/mp_level_map.csv'  INTO TABLE medicine.mp_level_map_txt;

drop table if exists medicine.mp_level_map;
CREATE TABLE  if not exists medicine.mp_level_map(
levelalias                string,
mp_hoslevel               string
)
STORED AS ORC;

insert into table medicine.mp_level_map select * from medicine.mp_level_map_txt;

	14、
drop table if exists medicine.mp_province_map_txt;
CREATE TABLE  if not exists medicine.mp_province_map_txt(
proid           string,
province        string,
mpprovince      string,
cityregion      string,
capital         string,
provinceshorts  string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY'\t';

LOAD DATA LOCAL INPATH '/mnt/disk6/das/mp_province_map.csv'  INTO TABLE medicine.mp_province_map_txt;

drop table if exists medicine.mp_province_map;
CREATE TABLE  if not exists medicine.mp_province_map(
proid           string,
province        string,
mpprovince      string,
cityregion      string,
capital         string,
provinceshorts  string
)
STORED AS ORC;

insert into table medicine.mp_province_map select * from medicine.mp_province_map_txt;

	15、
drop table if exists medicine.mp_disease_txt;
CREATE TABLE  if not exists medicine.mp_disease_txt(
disease_name                     string,
disease_category                 string,
disease_alias                    string,
disease_summary                  string,
icdid                            string,
admission_standards              string,
anatomical_structure             string,
auxiliary_examination            string,
clinical_manifestation           string,
complication                     string,
complications                    string,
diagnosis                        string,
diagnosis_differential_diagnosis string,
differential_diagnosis           string,
disease_harm                     string,
disease_englishname              string,
disease_mutations                string,
disease_outcome                  string,
disease_prevention               string,
disease_prognosis                string,
disease_type                     string,
diseasere_currence               string,
epidemiology                     string,
etiology                         string,
examination                      string,
expert_opinion                   string,
firstaid_measures                string,
follow_up                        string,
genetic_factors                  string,
highrisk_population              string,
hospitals_doctors                string,
incidence                        string,
indications                      string,
lab_examination                  string,
mechanism                        string,
medical_history                  string,
nosetiology                      string,
nursing                          string,
pathogens                        string,
pathogeny                        string,
pathology                        string,
pathophysiology                  string,
rehabilitation                   string,
research                         string,
spreadinfection                  string,
treatment                        string,
incubation                       string,
cure_rate                        string,
department                       string,
medical_insurance                string,
treatment_cycle                  string,
prepare                          string,
drug                             string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY'\t';

LOAD DATA LOCAL INPATH '/mnt/disk6/das/mp_disease.csv'  INTO TABLE medicine.mp_disease_txt;

drop table if exists medicine.mp_disease;
CREATE TABLE  if not exists medicine.mp_disease(
disease_name                     string,
disease_category                 string,
disease_alias                    string,
disease_summary                  string,
icdid                            string,
admission_standards              string,
anatomical_structure             string,
auxiliary_examination            string,
clinical_manifestation           string,
complication                     string,
complications                    string,
diagnosis                        string,
diagnosis_differential_diagnosis string,
differential_diagnosis           string,
disease_harm                     string,
disease_englishname              string,
disease_mutations                string,
disease_outcome                  string,
disease_prevention               string,
disease_prognosis                string,
disease_type                     string,
diseasere_currence               string,
epidemiology                     string,
etiology                         string,
examination                      string,
expert_opinion                   string,
firstaid_measures                string,
follow_up                        string,
genetic_factors                  string,
highrisk_population              string,
hospitals_doctors                string,
incidence                        string,
indications                      string,
lab_examination                  string,
mechanism                        string,
medical_history                  string,
nosetiology                      string,
nursing                          string,
pathogens                        string,
pathogeny                        string,
pathology                        string,
pathophysiology                  string,
rehabilitation                   string,
research                         string,
spreadinfection                  string,
treatment                        string,
incubation                       string,
cure_rate                        string,
department                       string,
medical_insurance                string,
treatment_cycle                  string,
prepare                          string,
drug                             string
)
STORED AS ORC;

insert into table medicine.mp_disease select * from medicine.mp_disease_txt;

	16、
drop table if exists medicine.mp_drug_txt;
CREATE TABLE  if not exists medicine.mp_drug_txt(
medicine_name                string,
medical_system               string,
comment                      string,
related_diseases             string,
phonetic                     string,
level1                       string,
level2                       string,
category                     string,
level3                       string,
otc                          string,
company_name                 string,
dosage                       string,
level4                       string,
store                        string,
trait                        string,
occupational_insurance       string,
matters                      string,
spec                         string,
component                    string,
adr                          string,
english_name                 string,
contraindication             string,
expiredate                   string,
package                      string,
certification_number         string,
interaction                  string,
indication                   string,
pharmacology_toxicology      string,
standard                     string,
pregnant_nurs                string,
okinetics                    string,
pregnancy_grade              string,
children                     string,
gerontism                    string,
overload                     string,
major_functions              string,
lactation_grade              string,
origin                       string,
efficiency                   string,
property                     string,
document_source              string,
ancient_sources              string,
ancient_books                string,
pharmacological_effects      string,
trade_name                   string,
latin_name                   string,
prepare                      string,
medicine_alias               string,
function_category            string,
manufacture_address          string,
import_certification_number  string,
clinical_trials              string,
warning                      string,
notes                        string,
differentiate                string,
client_corp                  string,
package_company              string,
usage                        string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY'\t';

LOAD DATA LOCAL INPATH '/mnt/disk6/das/mp_drug.csv'  INTO TABLE medicine.mp_drug_txt;

drop table if exists medicine.mp_drug;
CREATE TABLE  if not exists medicine.mp_drug(
medicine_name                string,
medical_system               string,
comment                      string,
related_diseases             string,
phonetic                     string,
level1                       string,
level2                       string,
category                     string,
level3                       string,
otc                          string,
company_name                 string,
dosage                       string,
level4                       string,
store                        string,
trait                        string,
occupational_insurance       string,
matters                      string,
spec                         string,
component                    string,
adr                          string,
english_name                 string,
contraindication             string,
expiredate                   string,
package                      string,
certification_number         string,
interaction                  string,
indication                   string,
pharmacology_toxicology      string,
standard                     string,
pregnant_nurs                string,
okinetics                    string,
pregnancy_grade              string,
children                     string,
gerontism                    string,
overload                     string,
major_functions              string,
lactation_grade              string,
origin                       string,
efficiency                   string,
property                     string,
document_source              string,
ancient_sources              string,
ancient_books                string,
pharmacological_effects      string,
trade_name                   string,
latin_name                   string,
prepare                      string,
medicine_alias               string,
function_category            string,
manufacture_address          string,
import_certification_number  string,
clinical_trials              string,
warning                      string,
notes                        string,
differentiate                string,
client_corp                  string,
package_company              string,
usage                        string
)
STORED AS ORC;

insert into table medicine.mp_drug select * from medicine.mp_drug_txt;

	17、
drop table if exists medicine.mp_hospital_map_txt;
CREATE TABLE  if not exists medicine.mp_hospital_map_txt(
hospitalname           string,
hospitalalias        string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY'\t';

LOAD DATA LOCAL INPATH '/mnt/disk6/das/mp_hospital_map.csv'  INTO TABLE medicine.mp_hospital_map_txt;

drop table if exists medicine.mp_hospital_map;
CREATE TABLE  if not exists medicine.mp_hospital_map(
hospitalname           string,
hospitalalias        string
)
STORED AS ORC;

insert into table medicine.mp_hospital_map select * from medicine.mp_hospital_map_txt;

	18、
drop table if exists medicine.mp_hospital_txt;
CREATE TABLE  if not exists medicine.mp_hospital_txt(
hospitalname           string,
hospitallevel          string,
province           string,
city           string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY'\t';

LOAD DATA LOCAL INPATH '/mnt/disk6/das/mp_hospital.csv'  INTO TABLE medicine.mp_hospital_txt;

drop table if exists medicine.mp_hospital;
CREATE TABLE  if not exists medicine.mp_hospital(
hospitalname           string,
hospitallevel          string,
province           string,
city           string
)
STORED AS ORC;

insert into table medicine.mp_hospital select * from medicine.mp_hospital_txt;


--- 19、

insert overwrite local directory "/home/data/tmp/dn_inquiryall_extended_0221"
row format delimited
fields terminated by "\001"
select
regexp_replace(id, "\\r", "")                  ,
regexp_replace(content, "\\r", "")            ,
regexp_replace(contenttype, "\\r", "")         ,
regexp_replace(floorid, "\\r", "")             ,
regexp_replace(postid, "\\r", "")              ,
regexp_replace(subject, "\\r", "")             ,
regexp_replace(title, "\\r", "")               ,
regexp_replace(url, "\\r", "")                 ,
regexp_replace(userid, "\\r", "")              ,
regexp_replace(username, "\\r", "")            ,
regexp_replace(usertype, "\\r", "")            ,
regexp_replace(year_moth, "\\r", ""),
regexp_replace(post_year, "\\r", ""),
regexp_replace(post_month, "\\r", ""),
regexp_replace(postdate, "\\r", "")            ,
regexp_replace(season, "\\r", "")              ,
regexp_replace(department, "\\r", "")          ,
regexp_replace(jobtitle, "\\r", "")            ,
regexp_replace(hospital , "\\r", "")           ,
regexp_replace(hospitallevel, "\\r", "")       ,
regexp_replace(city , "\\r", "")               ,
regexp_replace(citytier, "\\r", "")            ,
regexp_replace(province, "\\r", "")            ,
regexp_replace(usualregion, "\\r", "")         ,
regexp_replace(formalregion, "\\r", "")        ,
regexp_replace(platform, "\\r", "")
from test.dn_inquiryall_extended_0221;




drop table if exists extract.dn_inquiryall_extended_0221_txt;
CREATE TABLE  if not exists extract.dn_inquiryall_extended_0221_txt(
id                 string,
content            string,
contenttype        string,
floorid            string,
postid             string,
subject            string,
title              string,
url                string,
userid             string,
username           string,
usertype           string,
year_moth          string,
post_year          string,
post_month         string,
postdate           string,
season             string,
department         string,
jobtitle           string,
hospital           string,
hospitallevel      string,
city               string,
citytier           string,
province           string,
usualregion        string,
formalregion       string,
platform           string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY'\001';

LOAD DATA LOCAL INPATH '/mnt/disk6/das/dn_inquiryall_extended_0221.csv'  INTO TABLE extract.dn_inquiryall_extended_0221_txt;


drop table if exists das.dn_inquiryall_extended_0221;
CREATE TABLE  if not exists das.dn_inquiryall_extended_0221(
id                 string,
content            string,
contenttype        string,
floorid            string,
postid             string,
subject            string,
title              string,
url                string,
userid             string,
username           string,
usertype           string,
year_moth          string,
post_year          string,
post_month         string,
postdate           string,
season             string,
department         string,
jobtitle           string,
hospital           string,
hospitallevel      string,
city               string,
citytier           string,
province           string,
usualregion        string,
formalregion       string,
platform           string
)
CLUSTERED BY (id) INTO 113 BUCKETS
STORED AS orc;


insert into table das.dn_inquiryall_extended_0221 select * from extract.dn_inquiryall_extended_0221_txt;


--	20、

drop table if exists extract.michelle_lily_weibo_0118_txt;
CREATE TABLE  if not exists extract.michelle_lily_weibo_0118_txt(
id                     string,
wid                    bigint,
text                   string,
source                 string,
geo                    string,
pic_urls               string,
uid                    string,
province               bigint,
city                   bigint,
ulocation              string,
ugender                string,
ufollowers_count       bigint,
ufriends_count         bigint,
ustatuses_count        bigint,
ufavourites_count      bigint,
uverified              string,
verified_reason        string,
ubi_followers_count    bigint,
created_at             string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY'\t';


LOAD DATA LOCAL INPATH '/mnt/disk6/das/michelle_lily_weibo_0118.csv'  INTO TABLE extract.michelle_lily_weibo_0118_txt;


drop table if exists das.michelle_lily_weibo_0118;
CREATE TABLE  if not exists das.michelle_lily_weibo_0118(
id                     string,
wid                    bigint,
text                   string,
source                 string,
geo                    string,
pic_urls               string,
uid                    string,
province               bigint,
city                   bigint,
ulocation              string,
ugender                string,
ufollowers_count       bigint,
ufriends_count         bigint,
ustatuses_count        bigint,
ufavourites_count      bigint,
uverified              string,
verified_reason        string,
ubi_followers_count    bigint,
created_at             string
)
CLUSTERED BY (id) INTO 113 BUCKETS
STORED AS orc;


insert into table das.michelle_lily_weibo_0118 select * from extract.michelle_lily_weibo_0118_txt;


-- 21、

drop table if exists extract.michelle_lily_weibo_0118_attr_txt;
CREATE TABLE  if not exists extract.michelle_lily_weibo_0118_attr_txt(
id                     string,
attr                    string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY'\t';


LOAD DATA LOCAL INPATH '/mnt/disk6/das/michelle_lily_weibo_0118_attr.csv'  INTO TABLE extract.michelle_lily_weibo_0118_attr_txt;


drop table if exists das.michelle_lily_weibo_0118_attr;
CREATE TABLE  if not exists das.michelle_lily_weibo_0118_attr
STORED AS orc
as
select
*
from extract.michelle_lily_weibo_0118_attr_txt
;




-----删除所有临时表

drop table if exists das.das_testdata_20170224_txt;
drop table if exists das.lsk_6plat_0120_txt;
drop table if exists das.lsk_6plat_0120_attr_txt;
drop table if exists das.lsk_6plat_0221_txt;
drop table if exists das.lsk_weibo_cleaned_0221_txt;
drop table if exists das.lsk_weibo_cleaned_0221_attr_txt;
drop table if exists das.priorpregnancy_inquiry_2016_txt;
drop table if exists das.priorpregnancy_inquiry_2016_attr_txt;
drop table if exists medicine.mp_department_txt;
drop table if exists medicine.mp_department_map_txt;
drop table if exists medicine.mp_jobtitle_txt;
drop table if exists medicine.mp_jobtitle_map_txt;
drop table if exists medicine.mp_level_map_txt;
drop table if exists medicine.mp_province_map_txt;
drop table if exists medicine.mp_disease_txt;
drop table if exists medicine.mp_drug_txt;
drop table if exists medicine.mp_hospital_map_txt;
drop table if exists medicine.mp_hospital_txt;

drop table if exists extract.dn_inquiryall_extended_0221_txt;
drop table if exists extract.michelle_lily_weibo_0118_txt;
drop table if exists extract.michelle_lily_weibo_0118_attr_txt;








--------------------2、第二阶段









