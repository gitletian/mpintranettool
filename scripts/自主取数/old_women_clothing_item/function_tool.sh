#!/bin/sh

###################################################################################################
####                           公共方法
####
####
####
####################################################################################################
####################################带时间的日志信息####################################################################
function echo_info {
  NOW_DATE=`date '+%Y-%m-%d %H:%M:%S'`
  echo "${NOW_DATE}=============$1"
}

####################################带时间的日志信息####################################################################

################################# 结果处理函数 begin ###################################################################
#执行失败
function error_exit {

  echo_info "error=====$1"
  #curl -d "content=$1&token=Marcpoint_Data_Processing" http://172.16.1.138:8000/notification/shell_error/
  [ $2 -eq "1" ] && exit 1
}

#执行成功
function succeed_exit {
  echo_info "succeed_exit======$1"
  #curl -d "content=$1&token=Marcpoint_Data_Processing" http://172.16.1.138:8000/notification/shell_error/
  exit 1
}
####################################结果处理函数 end ###################################################################