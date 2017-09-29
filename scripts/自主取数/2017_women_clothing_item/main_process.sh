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


DATERANGE=`date -d '1 day ago' +%Y-%m-%d`
[ -n "$1" ] && DATERANGE=$1


echo_info "############################### begin ${DATERANGE} data process is start...... ##############################"
FILEDATE=${DATERANGE//-/_}
COMBINE_DATA_DIR=/home/data/combine_item_201708/
MJW_DATA_DIR=/home/data/mjw_item_201708/

###################################### ftp 数据 ######################################################
echo_info "------------------------------- ${DATERANGE} data dowload is start...... ------------------------------------"
[ -f "${COMBINE_DATA_DIR}/hy_item_${FILEDATE}.bz2" ] || wget --directory-prefix=$COMBINE_DATA_DIR  ftp://106.15.32.68/hy_item_${FILEDATE}.bz2 --ftp-user=user2 --ftp-password=******* || error_exit "ftp获取item_combine数据失败" "1"
[ -f "${MJW_DATA_DIR}/hy_item_${FILEDATE}.zip" ] || wget --directory-prefix=$MJW_DATA_DIR  ftp://115.231.103.216/hy_item_${FILEDATE}.zip --ftp-user=marcpoint --ftp-password=******* || error_exit "ftp获取item_combine数据失败" "1"


################################### item bak 数据处理 ########################################################
echo_info "------------------------------- upload  women_clothing_item file to hdfs  is start....... -------------------"
hdfs dfs -put ${COMBINE_DATA_DIR}/hy_item_${FILEDATE}.bz2 /bak/women_clothing_item/combine_item_201708/ || error_exit "$LINENO: ITEM文件归档失败" "0"
hdfs dfs -put ${MJW_DATA_DIR}/hy_item_${FILEDATE}.zip /bak/women_clothing_item/mjw_item_201708/ || error_exit "$LINENO: ITEM zip 文件归档失败" "0"


################################### item 数据处理 ########################################################

#处理item
echo_info "------------------------------- begin to process  women_clothing_item date  is start....... -----------------"
if [ -f "${COMBINE_DATA_DIR}/hy_item_${FILEDATE}.bz2" ];
then
   beeline -u jdbc:hive2://mphd02:10000 -n elengjing --hivevar daterange=${DATERANGE} --hivevar filedate=${FILEDATE} -f /home/script/normal_servers/script/women_clothing_item_import.sql || error_exit "$LINENO: ITEM数据入库处理失败" "1"
else
  error_exit "item zip is not exists" "1"
fi

################################### sqoop 导出数据到 pg ########################################################
echo_info "------------------------------- upload women_clothing_item date to pg is start....... -----------------------"
echo_info "upload women_clothing_item date to pg......."
sqoop --options-file /home/script/normal_servers/script/women_clothing_item_export.txt || error_exit "$LINENO: 导出 women_clothing_item 数据 到pg 失败" "1"

echo_info "upload women_clothing_item_unique date to pg......."
sqoop --options-file /home/script/normal_servers/script/women_clothing_item_unique_export.txt || error_exit "$LINENO: 导出 women_clothing_item_unique 数据 到pg 失败" "1"


echo_info "upload women_clothing_item_new_and_supply date to pg......."
sqoop --options-file /home/script/normal_servers/script/women_clothing_item_new_and_supply_export.txt || error_exit "$LINENO: 导出 women_clothing_item_new_and_supply 数据 到pg 失败" "1"


echo_info "upload shop_category_discountprice_export date to pg......."
sqoop --options-file /home/script/normal_servers/script/shop_category_discountprice_export.txt || error_exit "$LINENO: 导出 shop_category_discountprice 数据 到pg 失败" "1"

################################### build cube women_clothing_item_attr ########################################################
echo_info "------------------------------- build cube women_clothing_item is start....... ------------------------------"


STARTTIME=`date -d "${DATERANGE} 00:00:00+0000" +%s`
ENDTIME=`date -d "-1 day ago ${DATERANGE} 00:00:00+0000" +%s`

echo_info "start date = ${STARTTIME}000 ; end dat = ${ENDTIME}000"

echo_info "build women_clothing_item_attr start......"
curl -v -i -H "Content-Type: application/json;charset=UTF-8" --user ADMIN:*********** -XPUT http://mphd04:7070/kylin/api/cubes/women_clothing_item_attr_v2/rebuild -d "{\"startTime\" : ${STARTTIME}000  , \"endTime\" : ${ENDTIME}000, \"buildType\": \"BUILD\"}"
################################### build cube women_clothing_item_price ########################################################
echo_info "build women_clothing_item_price start......"
curl -v -i -H "Content-Type: application/json;charset=UTF-8" --user ADMIN:*********** -XPUT http://mphd04:7070/kylin/api/cubes/women_clothing_item_price_v2/rebuild -d "{\"startTime\" : ${STARTTIME}000  , \"endTime\" : ${ENDTIME}000, \"buildType\": \"BUILD\"}"


echo_info "############################### begin ${DATERANGE} data process is secuss...... #############################"

