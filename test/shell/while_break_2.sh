#!/bin/sh

###################################################################################################
####                           公共方法
####
####
####
####################################################################################################
####################################带时间的日志信息####################################################################
. /etc/profile



while [ true ];
do
    echo "test" >> /home/marcpoint/test/test.csv
    if [ $(wc -l /home/marcpoint/test/test.csv | awk '{print $1}') -gt 100 ] ;then
        touch /home/marcpoint/test/complete_flag
        break
    fi
    sleep 5s
done