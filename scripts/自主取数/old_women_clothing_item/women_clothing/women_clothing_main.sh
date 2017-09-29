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


## 获取时间参数
#YESTERDAY="2016_09_15"
YESTERDAY=`date -d -2days '+%Y_%m_%d'`
[ -n "$1" ] && YESTERDAY=$1
END_DATA=${YESTERDAY:0:4}-${YESTERDAY:5:2}-${YESTERDAY:8:2}

##############数据目录###########

DATA_DIR=/home/data/women_clothing_item

ZIP_ITEM_FILE=${DATA_DIR}/hy_item/hy_item_${YESTERDAY}.zip
CSV_ITEM_FILE=${DATA_DIR}/hy_item/hy_item_${YESTERDAY}.csv

ZIP_SHOP_FILE=${DATA_DIR}/hy_shop/hy_shop_${YESTERDAY}.zip
CSV_SHOP_FILE=${DATA_DIR}/hy_shop/hy_shop_${YESTERDAY}.csv

echo_info "**********************${YESTERDAY} women colthing  date process begin.....************************************"

##############hdfs数据归档目录###########

BAK_DIR=/bak/elengjing
ITEM_BAK_DIR=${BAK_DIR}/women_clothing_item/
SHOP_BAK_DIR=${BAK_DIR}/women_clothing_shop/

###################################### ftp 数据 ######################################################
echo_info "${YESTERDAY} data dowload flag............"

[ -f "$ZIP_SHOP_FILE" ] || wget --directory-prefix=${DATA_DIR}/hy_shop/  ftp://115.231.103.57/hy_shop/hy_shop_${YESTERDAY}.zip --ftp-user=marcpoint --ftp-password=********** || error_exit "ftp获取shop数据失败" "1"

[ -f "$ZIP_ITEM_FILE" ] || wget --directory-prefix=${DATA_DIR}/hy_item/  ftp://115.231.103.57/hy_item/hy_item_${YESTERDAY}.zip --ftp-user=marcpoint --ftp-password=********** || error_exit "ftp获取item数据失败" "1"


################################## shop数据处理 #######################################################
#必须先进行shop数据处理，在再进行item数据处理，item数据依赖shop数据
#处理shop
#zip数据归档
echo_info "shop process begin ...."
if [ -f "$ZIP_SHOP_FILE" ]; 
then
  echo_info "upload SHOP file to hdfs and unzip file....."
  kinit -kt ./keytab/hdfs.keytab hdfs || error_exit "$LINENO: hdfs  keytab is error" "0"
  sudo -u hdfs hdfs dfs -put ${ZIP_SHOP_FILE} ${SHOP_BAK_DIR} || error_exit "$LINENO: SHOP文件归档失败" "0"
  [ -f "${CSV_SHOP_FILE}" ] || unzip ${ZIP_SHOP_FILE} -d ${DATA_DIR}/hy_shop/ || error_exit "$LINENO: SHOP文件解压失败" "1"
else
  error_exit "shop zip is not exists" "1"
fi

#开始处理 CSV 数据
if [ -f "${CSV_SHOP_FILE}" ]; 
then
  echo_info "begin to exuexs shop sql file....."
  beeline -u jdbc:hive2://mphd02:10000 -n hive -p hive1 --hivevar CSV_SHOP_FILE=${CSV_SHOP_FILE} -f ./women_clothing/mp_women_clothing_shop.sql || error_exit "$LINENO: SHOP数据load hive失败" "1"
  ###########增加进总shop#####################
  beeline -u jdbc:hive2://mphd02:10000 -n hive -p hive1 -f ./mp_shop.sql || error_exit "$LINENO: shop 汇总失败" "1"
  
  ########sqoop 上传至pg##########
  echo_info "sqoop shop to pg....."
  kinit -kt ./keytab/sqoop.keytab sqoop || error_exit "$LINENO: sqoop  keytab is error" "0"
  sqoop --options-file ./shop_to_pg.txt || error_exit "$LINENO: shop 上传至 pg失败" "1"
  
  echo_info "load shop data ok!"
  #rm -rf ${ZIP_SHOP_FILE}
  rm -rf ${CSV_SHOP_FILE}
  echo_info "clear shop tmp data ok!"
 
else
  error_exit "shop csv is not exists" "1"
fi


################################### item 数据处理 ########################################################
#处理item
#zip数据归档

if [ -f "$ZIP_ITEM_FILE" ]; 
then
  echo_info "upload item file to hdfs and unzip file......."
  kinit -kt ./keytab/hdfs.keytab hdfs || error_exit "$LINENO: hdfs  keytab is error" "0"
  sudo -u hdfs hdfs dfs -put ${ZIP_ITEM_FILE} ${ITEM_BAK_DIR} || error_exit "$LINENO: ITEM文件归档失败" "0"
  [ -f "${CSV_ITEM_FILE}" ] || unzip ${ZIP_ITEM_FILE} -d ${DATA_DIR}/hy_item/ || error_exit "$LINENO: ITEM文件解压失败" "1"

else
  error_exit "item zip is not exists" "1"
fi

#开始处理 CSV 数据
if [ -f "${CSV_ITEM_FILE}" ]; 
then
  echo_info "begin to exuexs item sql file......."
  beeline -u jdbc:hive2://mphd02:10000 -n hive -p hive1 --hivevar CSV_ITEM_FILE=${CSV_ITEM_FILE} -f ./women_clothing/mp_item.sql || error_exit "$LINENO: ITEM数据load hive失败" "1"
  echo_info "load ITEM data ok!"
  #rm -rf ${ZIP_ITEM_FILE}
  rm -rf ${CSV_ITEM_FILE}
  echo_info "clear ITEM tmp data ok!"
  
else
  error_exit "item csv is not exists" "1"
fi



#####################################更新 pg industry table########################################################################
#beeline -u jdbc:hive2://mphd02:10000 -n hive -p hive1 -e 'select max(DateRange) from elengjing.women_clothing_item' > date.tmp
#TESTDATE=`sed -n 4,4p date.tmp`
#rm -rf date.tmp
#MAXDATE=${TESTDATE:2:10}
echo_info "begin to update end_date to pg industry table!"
kinit -kt ./keytab/sqoop.keytab sqoop || error_exit "$LINENO: sqoop  keytab is error" "0"
sqoop eval \
--connect jdbc:postgresql://192.168.110.11:5432/mp_portal \
--username elengjing \
--password Marcpoint2016 \
--query "update industry set end_date='${END_DATA}' where id=16"


succeed_exit "**********************${YESTERDAY} women colthing  date process sucess************************************"
