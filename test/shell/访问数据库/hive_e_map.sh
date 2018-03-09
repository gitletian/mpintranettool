#!/bin/sh



HIVE_RESULT=`beeline -u jdbc:hive2://yuanpei.guo:10000 -n guoyuanpei --showHeader=false --outputformat=csv2 -e "select id, user_id, platform_id, categoryid from transforms.tp_brand_price_attr limit 5"`

declare -A column_new=()
declare -A column_old=(['1005:141983']='1005:141983' ['1007:7483275']='1005:141983')
# 设置数组分隔符
echo "========================================================================================"

for row_str in $HIVE_RESULT;
do
    row=(${row_str//,/ })
    column_new[${row[1]}]=${row[1]}
done

echo "新表数据: ${column_new[*]}"

echo "旧表数据: ${column_old[*]}"


for r in ${!column_old[@]};
do
echo "需要提出的数据为: ${r}"
unset column_new[${r}]
done

echo ${column_new[*]}

