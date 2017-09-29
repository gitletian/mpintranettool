#!/bin/sh

###################################################################################################
####                                   定时任务主 程序
####
####
####
####################################################################################################
## 公用环境变量及方法生效
. /etc/profile
. ./function_tool.sh

##-----------------------目录名称, 依据日期date获取---------------------------#
#YESTERDAY="2016_09_15"
YESTERDAY=`date -d -2days '+%Y_%m_%d'`
[ -n "$1" ] && YESTERDAY=$1

###################################################------load udf source ------- #################################################
echo_info "#######################################${YESTERDAY} date begin##############################################"
beeline -u jdbc:hive2://mphd02:10000 -n hive -p hive1 -f ./serverudf/initudfenv.sql || error_exit "$LINENO: add all source file error" "0"

####################################################################################################
sh ./women_clothing/women_clothing_main.sh ${YESTERDAY} >> women_clothing_log.log

echo_info "#######################################${YESTERDAY} date end##############################################"
# sh ./women_clothing/women_clothing_main.sh ${YESTERDAY}

#time sh ./dried_milk/dried_milk_main.sh ${YESTERDAY} >> ./dried_milk/dried_milk_log.log
