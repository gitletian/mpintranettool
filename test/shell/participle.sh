#!/bin/sh

###################################################################################################
####                           判断远程文件是否处理成功
####
####
####
####################################################################################################
####################################带时间的日志信息####################################################################
. /etc/profile
date=$1
root_dir=/home/marcpoint/Desktop/sentiment_analysis_v2
flag_dir=${root_dir}/predict/${date}

echo "place wait, data is begin to process....."
ssh marcpoint@172.16.1.145 "sh ${root_dir}/helper.sh &" &

echo "place wait, data is processing....."

while [ true ];
do
    if [ $(ssh marcpoint@172.16.1.145 "test -f ${flag_dir}/complete_flag && echo true") ]; then
        echo "data process finnish."
        break
    fi
    echo "file is not exist"
    sleep 5s
done

echo "Congratulation, you are rigth!"