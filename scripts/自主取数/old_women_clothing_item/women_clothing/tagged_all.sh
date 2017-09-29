#!/bin/sh

###################################################################################################
####                           标签库更新,重新打标签
#### 1、对标签库进行更新
#### 2、导出最新标签的json
#### 3、对全量item进行重新打标签
####################################################################################################

## 环境变量生效
. /etc/profile
. ../function_tool.sh
echo_info "********************** 标签库更新,重新打全量标签 process begin.....************************************"
rm -rf /home/script/normal_servers/serverudf/elengjing/industryattr.json || error_exit "$LINENO: 删除旧标签失败" "0"
python /home/script/normal_servers/serverudf/elengjing/attr_script/get_industry_attr.py || error_exit "$LINENO: 新标签库生成失败" "1"
kinit -kt ../keytab/hdfs.keytab hdfs || error_exit "$LINENO: hdfs  keytab is error" "0"
sudo -u hive hdfs dfs -rm -r /data/industryattr.json || error_exit "$LINENO: 删除hdfs上旧标签失败" "0"
sudo -u hive hdfs dfs -put /home/script/normal_servers/serverudf/elengjing/industryattr.json /data/ || error_exit "$LINENO: 更新hdfs上旧标签失败" "1"
beeline -u jdbc:hive2://mphd02:10000 -n hive -p hive1 -f /home/script/normal_servers/women_clothing/mp_update_item_attr_all.sql || error_exit "$LINENO: 重新打标签失败" "1"
echo_info "********************** 标签库更新,重新打全量标签 process secuss************************************"