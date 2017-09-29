# coding: utf-8
from __future__ import unicode_literals

import sys
reload(sys)
sys.setdefaultencoding("utf-8")

import traceback
import re
import datetime
from dateutil.relativedelta import relativedelta

tempates = [
    (['未知', '未填写'], '未知'),
    (['还没有宝宝', '没有宝宝', '暂无宝宝', '准备怀孕', '备孕', '备孕中'], -600),
    (['孕', '孕妈', '孕期', '怀孕中'], '怀孕中'),
    (['家中有宝', '出生', '宝宝出生', '女宝刚出生'], '已有宝宝'),
    (['6岁以上', '宝宝6岁以上'], 2200),
]

be_born_rule = [
        ('.*?(?:宝宝)?(\d+)岁(\d+)个月(\d+)天', '{0} * 365 + {1} * 30 + {2}'),
        ('.*?(?:宝宝)?(\d+)岁(\d+)个月$', '{0} * 365 + {1} * 30'),
        ('.*?(?:宝宝)?(\d+)岁(\d+)天', '{0} * 365 + {1}'),
        ('.*?(?:宝宝)?(\d+)岁半', '{0} * 365 + 30 * 6'),
        ('.*?(?:宝宝)?(\d+)岁$', '{0} * 365'),

        ('.*?(?:宝宝)?(\d+)个月(\d+)天', '{0} * 30 + {1}'),
        ('.*?(?:宝宝)?(\d+)个月$', '{0} * 30'),

        ('.*?(?:宝宝)?(\d+)周(\d+)天', '{0} * 7 + {1}'),
        ('.*?(?:宝宝)?(\d+)周$', '{0} * 7'),

        ('.*?(?:宝宝)?(\d+)天', '{0}'),

     ]
not_be_born_rule = [
        ('.*(?:(?:孕期)|(?:孕))(\d+)岁(\d+)个月(\d+)天', '{0} * 365 + ({1} - 10) * 30 + {2}'),
        ('.*(?:(?:孕期)|(?:孕))(\d+)岁(\d+)个月$', '{0} * 365 + ({1} - 10) * 30'),
        ('.*(?:(?:孕期)|(?:孕))(\d+)岁(\d+)天', '{0} * 365 - 10 * 30 + {1}'),
        ('.*(?:(?:孕期)|(?:孕))(\d+)岁半', '{0} * 365 + (6 - 10) * 30'),
        ('.*(?:(?:孕期)|(?:孕))(\d+)岁$', '{0} * 365 - 10 * 30'),

        ('.*(?:(?:孕期)|(?:孕))(\d+)个月(\d+)天', '({0} - 10) * 30 + {1}'),
        ('.*(?:(?:孕期)|(?:孕))(\d+)个月$', '({0} - 10) * 30'),

        ('.*(?:(?:孕期)|(?:孕))(\d+)周(\d+)天', '({0} - 40) * 7 + {1}'),
        ('.*(?:(?:孕期)|(?:孕))(\d+)周$', '({0} - 40) * 7'),

        ('.*(?:(?:孕期)|(?:孕))(\d+)天', '{0} - 7 * 40'),

    ]

user_id, brief_intro, user_tags, user_name, detail_url, user_gender, user_birthday, user_age, user_level, baby_count, baby_info, baby_gender, baby_birthday, baby_agenow, ask_count, reply_count, post_count, reply_post_count, quality_post_count, best_answer_count, fans_count, following_count, device, address, tel, province, city, created_at, updated_at, noise, platform_id = [""] * 31


def export_data(baby_birthday, error_info):
    '''
    export data to hive client
    :param baby_birthday: 宝宝生日
    :param error_info:
    :return:
    '''
    print "\t".join([unicode(user_id), unicode(brief_intro), unicode(user_tags), unicode(user_name), unicode(detail_url), unicode(user_gender), unicode(user_birthday), unicode(user_age), unicode(user_level), unicode(baby_count), unicode(baby_info), unicode(baby_gender), unicode(baby_birthday), unicode(baby_agenow), unicode(ask_count), unicode(reply_count), unicode(post_count), unicode(reply_post_count), unicode(quality_post_count), unicode(best_answer_count), unicode(fans_count), unicode(following_count), unicode(device), unicode(address), unicode(tel), unicode(province), unicode(city), unicode(created_at), unicode(updated_at), unicode(noise), unicode(platform_id)])


def get_day(ss):
    '''
    解析宝宝生日
    :param ss: 宝宝信息字符串
    :return: 宝宝日期
    '''
    ss = ss.replace('-', '')
    day = "error"
    for tempate in tempates:
        if ss in tempate[0]:
            day = tempate[1]
            break

    if day != "error":
        return day

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


def get_birthday(baby_days, postdate, ss):
    '''
    获取宝宝生日
    :param baby_days: 宝宝孕期天数
    :param postdate:  发帖时间
    :param ss:
    :return: 宝宝生日
    '''
    if not str(baby_days).replace('-', '').isdigit():
        match = re.match('.*(\d{4}-\d{2}-\d{2}).*', ss)
        if not match:
            return 'unknow'

        birthday_day = match.groups()[0]
        if ss.find("怀孕") > -1:
            birthday_day = (datetime.datetime.strptime(birthday_day, "%Y-%m-%d") + relativedelta(months=10)).strftime("%Y-%m-%d")

        return birthday_day

    birthday = datetime.datetime.strptime(postdate, "%Y-%m-%d") - relativedelta(days=int(baby_days))
    return birthday.strftime("%Y-%m-%d")

if __name__ == '__main__':
    for line in sys.stdin:
        try:
            user_id, brief_intro, user_tags, user_name, detail_url, user_gender, user_birthday, user_age, user_level, baby_count, baby_info, baby_gender, baby_birthday, baby_agenow, ask_count, reply_count, post_count, reply_post_count, quality_post_count, best_answer_count, fans_count, following_count, device, address, tel, province, city, created_at, updated_at, noise, platform_id = [
                _.replace("\\N", "") for _ in line.strip().split('\t')]

            baby_days = ''
            baby_stages = ''
            if updated_at and baby_agenow and len(baby_birthday) < 9:
                crawdate_date = updated_at
                if len(updated_at) > 10:
                    crawdate_date = updated_at[0:10]

                baby_days = get_day(baby_agenow.replace('  >>  在线', '').replace(' | 在线', '').replace('+', '').replace('又', ''))

                baby_birthday = get_birthday(baby_days, crawdate_date, baby_agenow)

            export_data(baby_birthday, '')

        except Exception, e:
            erro_info = traceback.format_exc().decode().replace("\t", "  ").replace("\n", "  ;;;;")
            export_data("",  erro_info)






