
------ 3 dimdate 创建
drop table if exists extract.dimdate;
CREATE TABLE  if not exists extract.dimdate(
 dateid            int
,daterange         string
,datemmdd          string
,years             string
,month             int
,monthname         string
,day               int
,dayofyear         int
,weekdayname       string
,calendarweek      int
,formatteddate     string
,quarter           int
,quartal           string
,yearquartal       string
,yearmonth         string
,yearcalendarweek  string
,weekdaytype       string
,americanholiday   string
,austrianholiday   string
,canadianholiday   string
,period            string
,weekstart         string
,weekend           string
,weekperiod        string
,weekperiodmmdd    string
,monthstart        string
,monthend          string
,quarterstart      string
,quarterend        string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY',';

LOAD DATA LOCAL INPATH '/home/data/dimdate.csv' OVERWRITE INTO TABLE extract.dimdate;


drop table if exists elengjing.dimdate;
CREATE TABLE  if not exists elengjing.dimdate(
 dateid            int
,daterange         date
,datemmdd          string
,years             string
,month             int
,monthname         string
,day               int
,dayofyear         int
,weekdayname       string
,calendarweek      int
,formatteddate     string
,quarter           int
,quartal           string
,yearquartal       string
,yearmonth         string
,yearcalendarweek  string
,weekdaytype       string
,americanholiday   string
,austrianholiday   string
,canadianholiday   string
,period            string
,weekstart         date
,weekend           date
,weekperiod        string
,weekperiodmmdd    string
,monthstart        date
,monthend          date
,quarterstart      string
,quarterend        string
)
STORED AS ORC
;

insert into table elengjing.dimdate
select
 dateid
,to_date(daterange)
,datemmdd
,year
,month
,monthname
,day
,dayofyear
,weekdayname
,calendarweek
,formatteddate
,quarter
,quartal
,yearquartal
,yearmonth
,yearcalendarweek
,weekdaytype
,americanholiday
,austrianholiday
,canadianholiday
,period
,to_date(weekstart)
,to_date(weekend)
,weekperiod
,weekperiodmmdd
,to_date(monthstart)
,to_date(monthend)
,quarterstart
,quarterend
from
extract.dimdate
;
