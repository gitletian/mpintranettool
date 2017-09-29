#!/usr/bin/python
# -*- coding: utf-8 -*-
from __future__ import unicode_literals
import pandas as pd
from datetime import datetime
import time
import re
from datetime import timedelta
import math
from dateutil.parser import parse
import csv
###
### 当前支持计算平台：宝宝树，妈妈网，摇篮网
### 必须要有的字段列表：'channel','babybirthday','babyagethen','postdate'，'platform'
### 支持标注孕期 备孕期（怀孕前1年）孕晚期(怀孕0-3个月)；孕中期（4-6个月）；孕晚期（7-9个月）;
###              0-3个月；4-6个月；7-9个月；10-12个月；
###              1-2岁；2-3岁；3-4岁；4-5岁；5-6岁;6岁以上
###

#----------------------------------------------------------
### 自定义设置
## 设置需要计算孕期的文件路径
file_path = "mamanet15all.txt" 
## 设置输出文件路径
output_file_path = "mamanet15all-stage.txt" 
get_time = datetime(2016,9,6)  ## 设置数据抓取时间
output_setting = 'all'     ## 有两种输出模式:
                            ##“all”带原表输出，数据大时写出会比较慢，不用后期合并，适合小白操作；
                            ##“stage”只输出发帖时距离宝宝生日的天数（stage_day）和对应的孕期（baby_stage）

#----------------------------------------------------------


## 自定义函数
##---------------------------------------------------------
## 解析年龄数据
def caculateBabytimeDelta(x):
#     x = x.decode('utf8')
    pattern = re.compile(r'\d+') #匹配数字
    numbers = pattern.findall(x)
#     print numbers
    if pd.isnull(x):
        return "None"
    elif type(x) != unicode:
        return "None"
    elif x == "备孕中":
        days = -600
    elif (len(numbers) == 3):  # 匹配 XX岁XX个月XX天
        if (("岁" in x) & ("月" in x) & ("天" in x)):
            days = int(numbers[0])*365 + int(numbers[1])*30 + int(numbers[2])
    elif (len(numbers) == 2):
        if (('岁' in x) & ('月' in x)):
            days = int(numbers[0])*365 + int(numbers[1])*30
        elif (('岁' in x) & ('天' in x)):
            days = int(numbers[0])*365 + int(numbers[1])
        elif (('月' in x) & ('天' in x)):
            days = int(numbers[0])*30 + int(numbers[1])
        elif (('孕' in x) & ('周' in x)):
            days = -1*(40 - int(numbers[0]))*7 - int(numbers[1])
        elif (('孕' in x) & ('月' in x)):
            days = -1*((10- int(numbers[0])) * 30 - numbers[1])
        else:
            return "None"
    elif (len(numbers) == 1):
        if (('孕' in x) & ('周' in x)):
            days = -1*((40 - int(numbers[0]))*7)
        elif (('孕' in x) & ('月' in x)):
            days = -1*((10- int(numbers[0]))*30)
        elif ('岁' in x):
            days = int(numbers[0])*365
        elif ('月' in x):
            days = int(numbers[0])*30
        elif ('天' in x):
            days = int(numbers[0])
        else:
            return "None"
    else:
        return "None"
    return timedelta(days)

# 计算发帖时宝宝年龄
def caculateBabyStageDays(get_time,post_time,timeDelta,IsHost,IsPreg): # 计算发帖时baby所处的时期
    if timeDelta is "None":
        return "None"
    elif IsHost is True: #是楼主
        birthday = post_time - timeDelta
        stage_day = (post_time - birthday).days
    else: #不是楼主
        birthday = get_time - timeDelta
        if IsPreg is True:
            stage_day = (post_time - birthday).days
        else:
            try:
                stage_day = (get_time - birthday).days - (get_time - post_time).days
            except:
                print post_time,type(post_time)
    return stage_day

# 划分孕期
def findBabyStage(x):
    global result
    if isinstance(x,str):
        try:
            x = float(x)
        except:
            x = None
    level = [-900,-665,-300,-180,-90,0,90,180,270,360,720,1080,1440,1800,2160]
    levelName = ['备孕前','备孕期','孕早期', '孕中期', '孕晚期', '0-3个月','4-6个月',
                 '7-9个月','10-12个月','1-2岁','2-3岁','3-4岁','4-5岁','5-6岁']
    if (x< min(level)) or (x is None):
        result = None
    elif x> max(level):
        result = "6岁以上"
    else:
        for i in range(0,len(level)-1):
            if (x>= level[i]) & (x< level[i+1]):
                result = levelName[i]
    return result
##---------------------------------------------------------



if __name__ == '__main__':
    pass

    ## 数据读取
    data = pd.read_csv(file_path, sep = '\t',error_bad_lines=False, quoting=csv.QUOTE_NONE, encoding='utf-8', nrows=1000000,names=['channel','subject','contenttype','isbestanswer','ishost','url','postid','floorid','title','content','tags','userid','usertype','username','userprofileurl','gender','birthday','userlevel','location','babybirthday','babyagethen','postdate','userstate','replycount','viewcount','collectioncount','device','hospital','department','section','jobtitle','academictitle','speciality','likes','crawldate','platform'])
    #data = pd.read_csv(file_path, sep = ',',index_col=False,error_bad_lines=False, quoting=csv.QUOTE_NONE, encoding='utf-8', nrows =1000)  # test
    
    #data.platform = data.platform.fillna(0)
    # ## 提取需要计算的数据列
    data = data[['channel','babybirthday','title','content','userid','babyagethen','postdate','crawldate','platform']]
    #data.to_csv('mamanet15-test.txt',sep='\t',encoding='utf8')
    data.platform = data.platform.fillna(0)

    musk1= data.postdate.apply(lambda x: pd.isnull(x) == False)
    data=data[musk1]
    musk2= data.babyagethen.apply(lambda x: pd.isnull(x) == False)
    data=data[musk2]
    
    #print data[musk==True]
    ## 处理时间格式一致,储存在post_time中
    post_time = []
    for i in data.postdate:
        i = i[0:10]+' '+i[10:]
        try:
            post_time.append(parse(i))
        except:
            print i,'pstime err'
            post_time.append(None)

    crawldate = []
    for i in data.crawldate:
    #     i = i.replace("-","/")
        try:
            crawldate.append(parse(i))
        except:
            #crawldate.append(None)
            print i

    ## 处理孕期
    data_len = len(data)
    stage_days = []
    baby_stages = []
    print len(data)
    for i in range(0,data_len):
        try:
            if i in data.babyagethen:
                text= data.babyagethen[i]
            else:
                text=''
                continue
        except Exception,e:
            #print i,'error'
            #print e
            #print data[i]
            text=""
        if (data.channel[i] == 1) & (data.babybirthday[i] == None):
            IsHost = True
            #text = str(data.babyagethen[i]).decode("utf8")
        elif (int(data.platform[i]) == 1005):
            IsHost = False
            #text = str(data.babyagethen[i]).decode("utf8")
        else:
            IsHost = False
            #text = str(data.babybirthday[i]).decode("utf8")
            if data.channel[i] == 2:
                text = text[4:len(text)]
    #             print text
        timeDelta = caculateBabytimeDelta(text)
    #     print timeDelta
        if timeDelta == "None":
            stage_days.append("None")
            baby_stages.append("None")
        else:
            if timeDelta < timedelta(0):
                IsPreg = True
            else:
                IsPreg = False
            try:##############
                stage_day = caculateBabyStageDays(crawldate[i],post_time[i],timeDelta,IsHost,IsPreg)
                stage_days.append(stage_day)
                baby_stage = findBabyStage(stage_day)
                baby_stages.append(baby_stage)
            except:
                print i,'err'
                #baby_stages.append("None")
            

    ## 输出
    if output_setting == "stage":
        result = pd.DataFrame({"发帖时宝宝天数":stage_days,"发帖时宝宝阶段":baby_stages})
        # result = pd.DataFrame({"babyagethen": data.babyagethen, "发帖时宝宝天数":stage_days,"发帖时宝宝阶段":baby_stages})  # test
        result.to_csv(output_file_path,sep=str("\t"),index=None, encoding="utf8")
        print ">>>>>>>年龄标记完毕！"

    elif output_setting == "all":
        result = pd.DataFrame({"发帖时宝宝天数":stage_days,"发帖时宝宝阶段":baby_stages})
        result2 = pd.concat([data,result],axis = 1)
        result2.to_csv(output_file_path,sep=str("\t"),index=None, encoding="utf8")
        print ">>>>>>>年龄标记完毕！"

    else:
        print "请设置输出格式[all/stage]"


