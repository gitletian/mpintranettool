# coding: utf-8
from __future__ import unicode_literals

import sys
reload(sys)
sys.setdefaultencoding("utf-8")
import traceback
import numpy as np
import re

itemattrdesc, attr_names = list(np.random.choice([""], size=2))


def export_data(attr_name, attr_value, error_info):
    '''
    export data to hive client
    :param attr_name: 属性名
    :param attr_value: 属性值
    :param error_info:
    :return:
    '''
    print "\t".join([unicode(attr_name), unicode(attr_value), unicode(error_info)])


def parse_attr(itemattrdesc, attr_name):
    '''
    获取属性值
    :param itemattrdesc: 属性描述
    :param attr_name: 属性名
    :return:
    '''

    patterns = "(?:^(?:{0})|(?:.*;{1})):(.*?);.*".format(attr_name, attr_name)
    matched = re.match(patterns, itemattrdesc)
    if matched:
        value_list = []
        for value in matched.groups():
            value_list.extend(value.split(" "))
        return value_list

    return []



'''
main function
'''
for line in sys.stdin:
    try:
        itemattrdesc, attr_names = [_.replace("\\N", "") for _ in line.strip().split('\t')]

        for attr_name in attr_names.split(","):
            parse_value_list = parse_attr(itemattrdesc, attr_name)

            for attr_value in parse_value_list:
                export_data(attr_name, attr_value, "")

    except Exception, e:
        erro_info = traceback.format_exc().decode().replace("\t", "  ").replace("\n", "  ;;;;")
        export_data("", "", erro_info)



