#!/bin/sh

set -e
###################################################################################################
####                           公共方法
####
####
####
####################################################################################################
####################################带时间的日志信息####################################################################
. /etc/profile

date=$1
dir=/home/marcpoint/Desktop/sentiment_analysis_v2

categorys=(baby_diapers mother_health baby_milk baby_food baby_feedtool)

categorys=(baby_diapers)
for category in ${categorys[*]};
do
    dataset=data/${category}/data_to_train.txt
    testset=data/${category}/data_to_test.txt
    embedding_file_path=data/${category}/general2-300/reduced_vectors.txt
    rootdir=./predict/${date}/input/${category}

    python ${dir}/at_lstm.py --date=$date --category=$category --dataset=$dataset --testset=$testset --embedding_file_path=$embedding_file_path --rootdir=$rootdir
done

touch ${dir}/predict/${date}/output/complete_flag


