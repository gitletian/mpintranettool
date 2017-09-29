#!/bin/sh

###################################################################################################
####                           更新item中的shopname
#### 1、更新item中的shopname
####
####
####################################################################################################

## 环境变量生效
. /etc/profile
. ../function_tool.sh
echo_info "********************** 更新item shop name process begin.....************************************"
kinit -kt ../keytab/hdfs.keytab hdfs || error_exit "$LINENO: hdfs  keytab is error" "0"
beeline -u jdbc:hive2://mphd02:10000 -n hive -p hive1 -f /home/script/normal_servers/women_clothing/mp_update_item_shopname_all.sql || error_exit "$LINENO: 更新item shop name失败" "1"
echo_info "********************** 更新item shop name process secuss************************************"