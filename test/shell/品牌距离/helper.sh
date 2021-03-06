#!/usr/bin/env bash

set -e
. /etc/profile
###################################################################################################
####                           公共方法
####
####
####
####################################################################################################
####################################带时间的日志信息####################################################################

dir=/home/marcpoint/Desktop/sentiment_analysis_v2
date=$1
output=${dir}/predict/${date}/output
# if [ ! -d $output ]; then mkdir $output; fi
[ ! -d $output ] && mkdir $output

categorys=(baby_diapers mother_health baby_milk baby_food baby_feedtool baby_care baby_health)

for category in ${categorys[*]};
do
    dataset=data/${category}/data_to_train.txt
    testset=data/${category}/data_to_test.txt
    embedding_file_path=data/${category}/general2-300/reduced_vectors.txt
    rootdir=./predict/${date}/input/${category}

    if [ -d ${rootdir} ];
    then
        /home/marcpoint/anaconda3/bin/python ${dir}/at_lstm.py --date=$date --category=$category --dataset=$dataset --testset=$testset --embedding_file_path=$embedding_file_path --rootdir=$rootdir
        now_date=`date '+%Y-%m-%d %H:%M:%S'`
        echo  "${now_date}: ${date}-${category}预测成功!"
    fi

done

touch ${dir}/predict/${date}/complete_flag


