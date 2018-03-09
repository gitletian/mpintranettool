#!/bin/sh

###################################################################################################
####                           判断远程文件是否处理成功
####
####
####
####################################################################################################
####################################带时间的日志信息####################################################################
. /etc/profile
set -e

date=$1
root_dir=/home/marcpoint/Desktop/sentiment_analysis_v2
flag_dir=${root_dir}/predict/${date}
base_dir=/home/tmp/brand_distance


### 准备远程服务器文件
rm -rf ${base_dir}/input/*/.*_0.crc
ssh marcpoint@172.16.1.145 "test -d ${root_dir}/predict/${date} && rm -rf ${root_dir}/predict/${date};mkdir -p ${root_dir}/predict/${date}"
echo "begin scp file to 172.16.1.145........."
scp -r ${base_dir}/input marcpoint@172.16.1.145:${flag_dir}/


### 开始预测
echo "place wait, data is begin to process....."
ssh marcpoint@172.16.1.145 "cd ${root_dir} && bash -i helper.sh ${date}" &


### 检测预测结果
echo "place wait, data is processing....."

while [ true ];
do
    if [ $(ssh marcpoint@172.16.1.145 "test -f ${flag_dir}/complete_flag && echo true") ]; then
        echo "data process finnish."
        break
    fi
    now_date=`date '+%Y-%m-%d %H:%M:%S'`
    echo "${now_date}: process is runing! place warit...."
    sleep 30m
done

### 将预测结果回传
echo "Congratulation, you are rigth!"
rm -rf ${base_dir}/output
scp -r marcpoint@172.16.1.145:${flag_dir}/output ${base_dir}/
echo 'file scp sucess!'
