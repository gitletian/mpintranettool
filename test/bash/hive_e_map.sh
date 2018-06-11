#!/bin/sh
set -e

[ -n "$1" ] || (echo "请输入新增列的sql语句" && exit 1)
[ -n "$2" ] || (echo "请输入数据表的名称" && exit 1)

NEW_COLUMNS_SQL=$1
TABLE_COLUMN=$2

ADD_COLUMNS=./${TABLE_COLUMN##*.}_add_columns.sql


HIVE_NEW_COLUMN=`beeline -u jdbc:hive2://10.2.8.52:10000 -n wuxin02 -p wuxin02 --showHeader=false --outputformat=csv2 -e "$NEW_COLUMNS_SQL" `
HIVE_OLD_COLUMN=`beeline -u jdbc:hive2://10.2.8.52:10000 -n wuxin02 -p wuxin02 --showHeader=false --outputformat=csv2 -e "desc ${TABLE_COLUMN}"`

declare -A OLD_COLUMN=()
declare -A NEW_COLUMN=()

echo "===================================== 解析旧表字断 ==================================================="

for old_row_str in $HIVE_OLD_COLUMN;
do
    row=(${old_row_str//,/ })
    if [ ${row[0]} == 'NULL' ]; then break; fi

    echo "字断名: ${row[0]}"
    OLD_COLUMN[${row[0]}]=${row[0]}
done

echo "===================================== 解析新表字断 ==================================================="
for new_row_str in $HIVE_NEW_COLUMN;
do
    row=(${new_row_str//,/ })
    NEW_COLUMN[${row[0]}]=${row[0]}
done

echo "===================================== 剔除旧表字断 ==================================================="
for r in ${!OLD_COLUMN[@]};
do
echo "需要提出的数据为: ${r}"
unset NEW_COLUMN[${r}]
done

echo "===================================== add columns 输出字断 ==================================================="
test -f $ADD_COLUMNS && rm -rf $ADD_COLUMNS
touch $ADD_COLUMNS

columns=""
for column in ${!NEW_COLUMN[@]};
do
columns="${columns},${column} string "
done

echo "ALTER TABLE ${TABLE_COLUMN} ADD COLUMNS(${columns:1}) cascade;" >> $ADD_COLUMNS

# beeline -u jdbc:hive2://10.2.8.52:10000 -n wuxin02 -p wuxin02 -f '$ADD_COLUMNS'


