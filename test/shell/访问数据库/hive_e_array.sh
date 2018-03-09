#!/bin/sh



HIVE_RESULT=`beeline -u jdbc:hive2://yuanpei.guo:10000 -n guoyuanpei --showHeader=false --outputformat=csv2 -e "select id, user_id, platform_id, categoryid from transforms.tp_brand_price_attr limit 5"`

column_new=()
column_old=(1005:141983 1007:7483275)
# 设置数组分隔符
echo "========================================================================================"

for row_str in $HIVE_RESULT;
do
    row=(${row_str//,/ })
    column_new+=(${row[1]})
done

echo "done ok"
echo ${column_new[*]}
echo ${column_new[1]}