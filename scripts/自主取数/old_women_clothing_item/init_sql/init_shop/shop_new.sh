#!/bin/sh
## 公用环境变量及方法生效

## 环境变量生效
. /etc/profile
. ../../function_tool.sh

########################################数据初始化######################################################################

################################## shop数据处理 #######################################################
#必须先进行shop数据处理，在再进行item数据处理，item数据依赖shop数据
#处理shop
#zip数据归档
echo_info "shop process begin ...."

SHOP_DIR="/home/data/women_clothing_item/shop_new/"

for((q=2015;q<=2016;q++));
do

for((i=1;i<=12;i++));
do 
  if [ ${i} -lt 10 ];
  then
    MONTH=0${i}
  else
    MONTH=${i}
  fi
  for((j=1;j<=31;j++));
  do
    DAY=0
    if [ ${j} -lt 10 ];
    then
      DAY=0${j}
    else
      DAY=${j}
    fi
    CSV_SHOP_FILE=${SHOP_DIR}/hy_shop_${q}_${MONTH}_${DAY}.csv
    if [ -f "$CSV_SHOP_FILE" ];
    then
      DATE_RANGE="${q}-${MONTH}-${DAY}"
      echo_info "===process date====${DATE_RANGE_D}"
      beeline -u jdbc:hive2://mphd02:10000 -n hive -p hive1 --hivevar CSV_SHOP_FILE=${CSV_SHOP_FILE} --hivevar DATE_RANGE=${DATE_RANGE} -f ./women_clothing/shop_new.sql || error_exit "$LINENO: SHOP数据load hive失败" "1"
    fi
  done
done
done
