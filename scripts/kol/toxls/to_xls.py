# coding: utf-8
from __future__ import unicode_literals
import sys
reload(sys)
sys.setdefaultencoding("utf-8")

import pandas as pd
import json
import copy
import pdb


with open('weibo.json', 'rb') as f:
    line = f.read()
weibo_list = json.loads(line, encoding='utf8')


def list2map(data):
    map_data = {}
    total = 0
    if data:
        for _ in data:
            map_data.update({_['name']: _['value']})
            total += _['value']

    map_data.update({'total': total})
    return map_data


all_data = []
for weibo in weibo_list:
    fans_city = {}
    fans_gender = {}
    fans_gestation_data = {}
    if weibo.has_key('fans_city'):
        for _ in weibo['fans_city']:
            fans_city = list2map(weibo['fans_city'])

    if weibo.has_key('fans_gender'):
        for _ in weibo['fans_gender']:
            fans_gender = list2map(weibo['fans_gender'])

    if weibo.has_key('fans_gestation_data'):
        for _ in weibo['fans_gestation_data']:
            fans_gestation_data = list2map(weibo['fans_gestation_data'])

    data = dict(
        user_id=weibo['user_id'],
        user_name=weibo['user_name'],
        fans_city_A=fans_city.get('A', 0),
        fans_city_B=fans_city.get('B', 0),
        fans_city_C=fans_city.get('C', 0),
        fans_city_D=fans_city.get('D', 0),
        fans_city_wu=fans_city.get('wu', 0),
        fans_city_KEY=fans_city.get('KEY', 0),
        fans_gender_female=fans_gender.get('female', 0) * 1.0 / fans_gender.get('total', 1),
        fans_gender_male=fans_gender.get('male', 0) * 1.0 / fans_gender.get('total', 1),
        fans_gender_wu=fans_gender.get('wu', 0) * 1.0 / fans_gender.get('total', 1),
        fans_gestation_data=fans_gestation_data.get('新生妈妈', 0) * 1.0 / fans_gestation_data.get('total', 1),
    )
    all_data.append(data)

df = pd.DataFrame(all_data)
df.to_excel('data.xls')