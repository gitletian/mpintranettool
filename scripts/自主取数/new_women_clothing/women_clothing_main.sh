#!/bin/sh

###################################################################################################
####                           women_clothing 模块 数据入库程序
####
####
####
####################################################################################################

## 环境变量生效
. /etc/profile
. ./function_tool.sh

########################################数据初始化######################################################################

BEGIN_DATE=2017-01-03
## 获取时间参数
#YESTERDAY="2016_09_15"
YESTERDAY=`date '+%Y-%m-%d'`
[ -n "$1" ] && YESTERDAY=$1
NEXT_DATA=${YESTERDAY:0:4}-${YESTERDAY:5:2}-${YESTERDAY:8:2}
END_DATA=`date -d "1 day ago $NEXT_DATA" +%Y-%m-%d`
FILE_DATA=${YESTERDAY:0:4}_${YESTERDAY:5:2}_${YESTERDAY:8:2}


##############数据目录###########

DATA_DIR=/home/data/women_clothing_item/item

ZIP_ITEM_FILE=${DATA_DIR}/hy_item_${FILE_DATA}.zip
CSV_ITEM_FILE=${DATA_DIR}/hy_item_${FILE_DATA}.csv

echo_info "**********************${YESTERDAY} women colthing  date process begin.....************************************"

##############hdfs数据归档目录###########

BAK_DIR=/bak/elengjing
ITEM_BAK_DIR=${BAK_DIR}/item/

###################################### ftp 数据 ######################################################
echo_info "${YESTERDAY} data dowload flag............"

[ -f "$ZIP_ITEM_FILE" ] || wget --directory-prefix=${DATA_DIR}/  ftp://106.15.32.68/hy_item_${FILE_DATA}.zip --ftp-user=user1 --ftp-password=Marcpointuser1 || error_exit "ftp获取item数据失败" "1"


################################### item 数据处理 ########################################################
#处理item
#zip数据归档

if [ -f "$ZIP_ITEM_FILE" ]; 
then
  echo_info "upload item file to hdfs and unzip file......."
  kinit -kt ./keytab/hdfs.keytab hdfs || error_exit "$LINENO: hdfs  keytab is error" "0"
  sudo -u hdfs hdfs dfs -put ${ZIP_ITEM_FILE} ${ITEM_BAK_DIR} || error_exit "$LINENO: ITEM文件归档失败" "0"
  [ -f "${CSV_ITEM_FILE}" ] || unzip ${ZIP_ITEM_FILE} -d ${DATA_DIR}/ || error_exit "$LINENO: ITEM文件解压失败" "1"

else
  error_exit "item zip is not exists" "1"
fi

#开始处理 CSV 数据
if [ -f "${CSV_ITEM_FILE}" ]; 
then
  echo_info "begin to exuexs item sql file......."
  beeline -u jdbc:hive2://mphd02:10000 -n hive -p hive1 --hivevar CSV_ITEM_FILE=${CSV_ITEM_FILE} -f ./zzqs/per_day_price.sql --hivevar BEGIN_DAY=${BEGIN_DATE}  --hivevar DATE_RANGE=${END_DATA} || error_exit "$LINENO: ITEM数据load hive失败" "1"
  echo_info "load ITEM data ok!"
  #rm -rf ${ZIP_ITEM_FILE}
  rm -rf ${CSV_ITEM_FILE}
  echo_info "clear ITEM tmp data ok!"
  
else
  error_exit "item csv is not exists" "1"
fi


################################## shop数据处理 #######################################################
#处理shop
#zip数据归档
echo_info "shop process begin ...."

  ###########对shop进行提取#####################
  ## beeline -u jdbc:hive2://mphd02:10000 -n hive -p hive1 -f ./zzqs/mp_shop.sql || error_exit "$LINENO: shop 汇总失败" "1"

  ########sqoop 上传至pg##########
  echo_info "sqoop shop to pg....."
  ## kinit -kt ./keytab/sqoop.keytab sqoop || error_exit "$LINENO: sqoop  keytab is error" "0"
  #sqoop --options-file ./zzqs/shop_to_pg.txt || error_exit "$LINENO: shop 上传至 pg失败" "1"

echo_info "load shop data ok!"

echo_info "shop process end ...."



################################## 合并item #######################################################

echo_info "begin to exuexs combine old item sql file......."
## beeline -u jdbc:hive2://mphd02:10000 -n hive -p hive1 --hivevar CSV_ITEM_FILE=${CSV_ITEM_FILE} -f ./zzqs/combine_old_item.sql  --hivevar DATE_RANGE=${END_DATA} || error_exit "$LINENO: ITEM数据合并失败" "1"
echo_info "combine old ITEM  ok!"



echo_info "**********************${YESTERDAY} women colthing  date process end.....************************************"