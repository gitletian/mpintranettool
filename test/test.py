# coding: utf-8
# __author__: ""
from __future__ import unicode_literals
import datetime
from dateutil.relativedelta import relativedelta
import pdb


min_date = "2015-10"
max_date = "2016-07"


def get_daterange_list(min_date, max_date, type="months"):
    '''
    获取月时间差
    :param min_date: 最小时间
    :param max_date: 最大时间
    :param type: months 或者 days
    :return:
    '''
    format = "%Y-%m-%d"
    if type == "months":
        format = "%Y-%m"

    min_date = datetime.datetime.strptime(min_date, format)
    max_date = datetime.datetime.strptime(max_date, format)
    i = 0
    daterange_list = []
    while True:
        daterange = min_date + relativedelta(**{type: i})
        if max_date < daterange:
            break

        daterange_list.append(daterange.strftime(format))
        i += 1
    return daterange_list
'''
print get_daterange_list(min_date, max_date, type="months")

'''
ss = [{"shopid": "69302618", "daterange": '2015-11', "cc": 13, "dd": 20}, {"shopid": "66098091", "daterange": '2016-02', "cc": 13, "dd": 23}
    , {"shopid": "66098091", "daterange": '2016-02', "cc": 14, "dd": 23}, {"shopid": "66098091", "daterange": '2016-02', "cc": 13, "dd": 23}
    , {"shopid": "66098091", "daterange": '2016-02', "cc": 13, "dd": 23}, {"shopid": "66098091", "daterange": '2016-02', "cc": 13, "dd": 23}
    , {"shopid": "66098091", "daterange": '2016-02', "cc": 13, "dd": 23}, {"shopid": "66098091", "daterange": '2016-02', "cc": 13, "dd": 23}
    , {"shopid": "66098091", "daterange": '2016-02', "cc": 13, "dd": 23}, {"shopid": "66098091", "daterange": '2016-02', "cc": 13, "dd": 23}]

ss = [{"shopid": "69302618", "daterange": '2015-11', "cc": 13, "dd": 20}, {"shopid": "66098091", "daterange": '2016-02', "cc": 13, "dd": 23}
    , {"shopid": "66098091", "daterange": '2016-02', "cc": 14, "dd": 23}]

shopid = ["66098091", "69302618", "73401272"]
date_list = ['2015-10', '2015-11', '2015-12', '2016-01', '2016-02', '2016-03', '2016-04', '2016-05', '2016-06', '2016-07']

import pandas as pd
import numpy as np
import itertools
xx = list(itertools.product(shopid, date_list))
jc = pd.DataFrame(xx, columns=["shopid", "daterange"])
data = pd.DataFrame(ss)
all = jc.merge(data, how="left", on=["shopid", "daterange"]).fillna(0)





