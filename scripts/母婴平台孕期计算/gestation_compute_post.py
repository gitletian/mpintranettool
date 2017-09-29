# coding: utf-8
from __future__ import unicode_literals

import sys
reload(sys)
sys.setdefaultencoding("utf-8")

import traceback
import re


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


id, channel, subject, post_id, title, tags, reply_count, view_count, collection_count, detail_url, content, is_best_answer, like_count, user_id, user_name, user_type, is_host, replied_user_id, replied_user_name, created_at, device, updated_at, baby_agethen, baby_days, floorid, noise, platform_id = [""] * 27


def export_data(baby_days, subject, error_info):
    '''
    export data to hive client
    :param baby_days: 发帖时宝宝天数
    :param subject: 主题
    :param error_info:
    :return:
    '''
    print "\t".join([unicode(id), unicode(channel), unicode(subject), unicode(post_id), unicode(title), unicode(tags), unicode(reply_count), unicode(view_count), unicode(collection_count), unicode(detail_url), unicode(content), unicode(is_best_answer), unicode(like_count), unicode(user_id), unicode(user_name), unicode(user_type), unicode(is_host), unicode(replied_user_id), unicode(replied_user_name), unicode(created_at), unicode(device), unicode(updated_at), unicode(baby_agethen), unicode(baby_days), unicode(floorid), unicode(noise), unicode(error_info), unicode(platform_id)])


def get_day(ss):
    '''
    解析宝宝生日
    :param ss: 宝宝信息字符串
    :return: 宝宝日期
    '''
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


if __name__ == '__main__':
    for line in sys.stdin:
        try:
            ss = [_.replace("\\N", "") for _ in line.strip().split('\t')]

            id, channel, subject, post_id, title, tags, reply_count, view_count, collection_count, detail_url, content, is_best_answer, like_count, user_id, user_name, user_type, is_host, replied_user_id, replied_user_name, created_at, device, updated_at, baby_agethen, baby_days, floorid, noise, platform_id = ss

            if baby_agethen:
                baby_days = get_day(baby_agethen.replace('  >>  在线', '').replace(' | 在线', '').replace('+', '').replace('又', '').replace('-', ''))

            if user_id and not user_id.startswith(platform_id + ':'):
                user_id = platform_id + ':' + user_id

            if replied_user_id and not replied_user_id.startswith(platform_id + ':'):
                replied_user_id = platform_id + ':' + replied_user_id

            export_data(baby_days, subject, '')

        except Exception, e:
            erro_info = traceback.format_exc().decode().replace("\t", "  ").replace("\n", "  ;;;;")
            export_data("", "", erro_info)







