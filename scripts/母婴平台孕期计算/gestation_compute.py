# coding: utf-8
from __future__ import unicode_literals

import sys
reload(sys)
sys.setdefaultencoding("utf-8")

import numpy as np
import traceback
import re
import datetime
from dateutil.relativedelta import relativedelta

tempate = ['宝宝6岁以上', '未知', '还没有宝宝', '怀孕中', '家中有宝', '正在孕期', '已有宝宝出生', '出生']

be_born_rule = [
        ('.*(?:宝宝)?(\d+)岁(\d+)个月(\d+)天', '({0} * 12 + {1}) * 30 + {2}'),
        ('.*(?:宝宝)?(\d+)岁(\d+)个月$', '({0} * 12 + {1}) * 30'),
        ('.*(?:宝宝)?(\d+)岁(\d+)天', '{0} * 365 + {1}'),
        ('.*(?:宝宝)?(\d+)岁半', '{0} * 365 + 30 * 6'),
        ('.*(?:宝宝)?(\d+)岁$', '{0} * 365'),

        ('.*(?:宝宝)?(\d+)个月(\d+)天', '{0} * 30 + {1}'),
        ('.*(?:宝宝)?(\d+)个月$', '{0} * 30'),

        ('.*(?:宝宝)?(\d+)周(\d+)天', '{0} * 7 + {1}'),
        ('.*(?:宝宝)?(\d+)周$', '{0} * 7'),

        ('.*(?:宝宝)?(\d+)天', '{0}'),

     ]
not_be_born_rule = [
        ('.*(?:(?:孕期)|(?:孕))(\d+)岁(\d+)个月(\d+)天', '({0} * 12 + {1} - 10) * 30 + {2}'),
        ('.*(?:(?:孕期)|(?:孕))(\d+)岁(\d+)个月$', '({0} * 12 + {1} - 10) * 30'),
        ('.*(?:(?:孕期)|(?:孕))(\d+)岁(\d+)天', '({0} * 12 - 10) * 30 + {1}'),
        ('.*(?:(?:孕期)|(?:孕))(\d+)岁半', '({0} * 12 + 6 - 10) * 30'),
        ('.*(?:(?:孕期)|(?:孕))(\d+)岁$', '({0} * 12 - 10) * 30'),

        ('.*(?:(?:孕期)|(?:孕))(\d+)个月(\d+)天', '({0} - 10) * 30 + {1}'),
        ('.*(?:(?:孕期)|(?:孕))(\d+)个月$', '({0} - 10) * 30'),

        ('.*(?:(?:孕期)|(?:孕))(\d+)周(\d+)天', '({0} - 40) * 7 + {1}'),
        ('.*(?:(?:孕期)|(?:孕))(\d+)周$', '({0} - 40) * 7'),

        ('.*(?:(?:孕期)|(?:孕))(\d+)天', '{0} - 7 * 40'),

    ]

level = [-900, -665, -300, -180, -90, 0, 90, 180, 270, 360, 720, 1080, 1440, 1800, 2160]
levelName = ['备孕前', '备孕期', '孕早期', '孕中期', '孕晚期', '0-3个月', '4-6个月', '7-9个月',' 10-12个月', '1-2岁', '2-3岁', '3-4岁', '4-5岁', '5-6岁']

channel, subject, contenttype, isbestanswer, ishost, url, postid, floorid, title, content, tags, userid, usertype, username, userprofileurl, gender, birthday, userlevel, location, babybirthday, babyagethen, postdate, userstate, replycount, viewcount, collectioncount,device, hospital, department, section, jobtitle, academictitle, speciality, likes, season, crawldate, platform = list(np.random.choice([""], size=37))


def export_data(baby_days, baby_stages, baby_birthday, error_info):
    '''
    export data to hive client
    :param baby_days: 发帖时宝宝天数
    :param baby_stages: 发帖时宝宝阶段
    :param baby_birthday: 宝宝生日
    :param error_info:
    :return:
    '''
    print "\t".join([unicode(channel), unicode(subject), unicode(contenttype), unicode(isbestanswer), unicode(ishost), unicode(url), unicode(postid), unicode(floorid), unicode(title), unicode(content), unicode(tags), unicode(userid), unicode(usertype), unicode(username), unicode(userprofileurl), unicode(gender), unicode(birthday), unicode(userlevel), unicode(location), unicode(babybirthday), unicode(babyagethen), unicode(postdate), unicode(userstate), unicode(replycount), unicode(viewcount), unicode(collectioncount), unicode(device), unicode(hospital), unicode(department), unicode(section), unicode(jobtitle), unicode(academictitle), unicode(speciality), unicode(likes), unicode(season), unicode(crawldate), unicode(baby_days), unicode(baby_stages), unicode(baby_birthday), unicode(error_info), unicode(platform)])


def get_day(ss):
    '''
    解析宝宝生日
    :param ss: 宝宝信息字符串
    :return: 宝宝日期
    '''
    if ss in tempate:
        return ss

    if ss == '备孕中':
        return '-600'

    day = 'error'

    rules = be_born_rule
    if re.match('.*((孕期)|孕)\d+.*', ss):
        rules = not_be_born_rule

    for rule in rules:
        match = re.match(rule[0], ss)
        if not match:
            continue

        param = match.groups()
        day = eval(rule[1].format(*param))
        break
    return str(day)


def find_baby_stage(baby_days):
    '''
    划分孕期阶段
    :param baby_days:
    :return:
    '''
    if not baby_days.replace('-', '').isdigit():
        return 'unknow'

    baby_days = int(baby_days)
    if baby_days< min(level):
        return 'unknow'

    if baby_days > max(level):
        return "6岁以上"

    result = 'unknow'
    for i in range(0, len(level)-1):
        if (baby_days >= level[i]) & (baby_days < level[i+1]):
            result = levelName[i]
            break

    return result


def get_birthday(baby_days, postdate):
    '''
    获取宝宝生日
    :param baby_days: 宝宝孕期天数
    :param postdate:  发帖时间
    :return: 宝宝生日
    '''
    if not baby_days.replace('-', '').isdigit():
        return 'unknow'

    birthday = datetime.datetime.strptime(postdate, "%Y-%m-%d") - relativedelta(days=int(baby_days))
    baby_birthday = birthday.strftime("%Y-%m-%d")

    return baby_birthday

if __name__ == '__main__':
    for line in sys.stdin:
        try:
            channel, subject, contenttype, isbestanswer, ishost, url, postid, floorid, title, content, tags, userid, usertype, username, userprofileurl, gender, birthday, userlevel, location, babybirthday, babyagethen, postdate, userstate, replycount, viewcount, collectioncount, device, hospital, department, section, jobtitle, academictitle, speciality, likes, season, crawldate, platform = [
                _.replace("\\N", "") for _ in line.strip().split('\t')]

            baby_days = ''
            baby_stages = ''
            baby_birthday = ''
            if platform in ['1001', '1003', '1004', '1005', '1008', '1015'] and postdate and crawldate:
                postdate_date = postdate
                if len(postdate) > 10:
                    postdate_date = postdate[0:10]

                dr = datetime.datetime.strptime(crawldate, "%Y-%m-%d") - datetime.datetime.strptime(postdate_date, "%Y-%m-%d")
                if babybirthday:
                    baby_days = get_day(babybirthday.replace('  >>  在线', '').replace(' | 在线', ''))
                    if baby_days.replace('-', '').isdigit():
                        baby_days = str(int(baby_days) - dr.days)
                elif babyagethen:
                    baby_days = get_day(babyagethen.replace('  >>  在线', '').replace(' | 在线', ''))

                baby_stages = find_baby_stage(baby_days)
                baby_birthday = get_birthday(baby_days, postdate_date)

            export_data(baby_days, baby_stages, baby_birthday, '')

        except Exception, e:
            erro_info = traceback.format_exc().decode().replace("\t", "  ").replace("\n", "  ;;;;")
            export_data("", "", "",  erro_info)






