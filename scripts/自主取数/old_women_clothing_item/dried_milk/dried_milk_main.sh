#!/bin/sh

################################# 结果处理函数 begin ###################################################################
#执行失败
function error_exit {

#  echo "$1" 1>&2
  echo "==========error_exit==============$1"
  #curl -d "content=$1&token=Marcpoint_Data_Processing" http://172.16.1.138:8000/notification/shell_error/
  [ $2 -eq "1" ] && exit 1
}

#执行成功
function succeed_exit {

  echo "==========succeed_exit==============$1"
  #curl -d "content=$1&token=Marcpoint_Data_Processing" http://172.16.1.138:8000/notification/shell_error/
  exit 1
}
###################################结果处理函数 end ###############################################################################


#########################################数据初始化######################################################################


## 获取时间参数
#YESTERDAY="2016_09_15"
YESTERDAY=`date -d -1days '+%Y_%m_%d'`
#RUN_TYPE：  1:服务器（1、ftp文件，2、上传hdfs备份）  ； 1:本地运行（1、运行本地文件，2、不上传hdfs）
RUN_TYPE=0
[ -n "$1" ] && YESTERDAY=$1
[ -n "$1" ] && RUN_TYPE=$2

## 数据目录#####
DATA_DIR=/home/data/dried_milk

ZIP_DRIED_MILK_FILE=${DATA_DIR}/hy_item/hy_dried_milk_${YESTERDAY}.zip
CSV_DRIED_MILK_FILE=${DATA_DIR}/hy_item/hy_dried_milk_${YESTERDAY}.csv


##hdfs 数据归档目录
BAK_DIR=/bak/elengjing
DRIED_MILK_BAK_DIR=${BAK_DIR}/dried_milk/

################################### item 数据处理 ########################################################

###################################### ftp 数据 ######################################################
echo "=============================================ftp dowload file ............===================================================="
[ -f "$ZIP_DRIED_MILK_FILE" ] || wget --directory-prefix=${DATA_DIR}/  ftp://115.231.103.57/hy_dried_milk/hy_dried_milk_${YESTERDAY}.zip --ftp-user=marcpoint --ftp-password=d7hN^AG:zj8v || error_exit "ftp获取item数据失败" "1"

#zip数据归档

if [ -f "$ZIP_DRIED_MILK_FILE" ]; 
then
  echo "=======ZIP_DRIED_MILK_FILE======upload file to hdfs and unzip file======================"
  kinit -kt ./keytab/hdfs.keytab hdfs && hdfs dfs -put ${ZIP_DRIED_MILK_FILE} ${DRIED_MILK_BAK_DIR} || error_exit "$LINENO: DRIED MILK ITEM文件归档失败" "0"
  unzip ${ZIP_DRIED_MILK_FILE} -d ${DATA_DIR}/dried_milk/ || error_exit "$LINENO: DRIED MILK ITEM文件解压失败" "1"
  echo "==ITEM===CSV_ITEM_FILE=${CSV_DRIED_MILK_FILE}==YESTERDAY=${YESTERDAY}===="
else
  error_exit "item  dried_milk zip is not exists" "1"
fi


#开始处理 CSV 数据
if [ -f "${CSV_DRIED_MILK_FILE}" ]; 
then
  echo "=======ZIP_DRIED_MILK_FILE======begin to exuexs sql file======================"
  beeline -u jdbc:hive2://mphd02:10000 -n hive -p hive1 --hivevar CSV_DRIED_MILK_FILE=${CSV_DRIED_MILK_FILE} -f ./dried_milk/dried_milk_item.sql || error_exit "$LINENO: DRIED MILK ITEM数据load hive失败" "1"
  
  beeline -u jdbc:hive2://mphd02:10000 -n hive -p hive1 -f ../mp_shop.sql || error_exit "$LINENO: shop 汇总失败" "1"
  
  
  ########sqoop 上传至pg##########
  echo "=======ZIP_DRIED_MILK_FILE======sqoop shop to pg======================"
  kinit -kt ./keytab/sqoop.keytab sqoop && sqoop --options-file ./shop_to_pg.txt
  
  echo "================load ITEM data ok======================="
  #rm -rf ${ZIP_DRIED_MILK_FILE}
  rm -rf ${CSV_DRIED_MILK_FILE}
  echo "=============clear ITEM DRIED MILK tmp data ok======================"
  succeed_exit "${YESTERDAY} DRIED MILK 数据处理成功"
  
else
  error_exit "item  dried_milk zip is not exists" "1"
fi



