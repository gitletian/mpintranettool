#!/bin/sh
###################################################################################################
####                           计算 mjw 前 N 天 销量之和(只计算前30天的)
#### 1、BEGIN_DATA:
#### 2、运行 sh:  sh mjw_qut_sum.sh "2017-01-03"
####
####################################################################################################
. /etc/profile

BEGIN_DATA=$1

for i in {1..29};
do
echo $i
CURRENT_DAY=`date -d "$[1-i] day ago $BEGIN_DATA" +%Y-%m-%d`
echo $CURRENT_DAY
beeline -u jdbc:hive2://mphd02:10000 -n hive -p hive1 --hivevar CURRENT_DAY=${CURRENT_DAY}  --hivevar INDEX=${i} -f ./mjw_qut_sum.sql
done;                                             
