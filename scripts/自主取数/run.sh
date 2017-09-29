#!/bin/sh
###################################################################################################
####                           根据 mjw 和自主抓取的月销量,计算出 日销量(使用于前30天数据)
####   1、BEGIN_DATA: 开始计算日期(从第一天开始)
####   2、DAY_NUM: 计算多少天的
####
####################################################################################################
. /etc/profile


for i in {1..14};
do
CURRENT_DAY=`date -d "-$[i] day ago 2017-01-03 " +%Y-%m-%d`
echo $CURRENT_DAY
YESTORY_DAY=`date -d "1 day ago $CURRENT_DAY" +%Y-%m-%d`
echo $YESTORY_DAY
beeline -u jdbc:hive2://mphd02:10000 -n hive -p hive1 --hivevar BEGIN_DATE=${BEGIN_DATA} --hivevar END_DATE=${YESTORY_DAY} --hivevar CURRENT_DATE=${CURRENT_DAY}  -f ./day_qut.sql
done;
