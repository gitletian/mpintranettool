# coding: utf-8
from __future__ import unicode_literals
import sys
reload(sys)
sys.setdefaultencoding("utf-8")

import pandas as pd
import json
import copy
import pdb


def dicttolsit(jsondata):
    list_data = []
    for k, v in jsondata.iteritems():
        if v != 0:
            list_data.append({"name": k, "value": v})

    return list_data


##################################  attention_data

product = pd.read_csv("/Users/guoyuanpei/workspace/pworkspace/mpintranettool/scripts/kol/weixin/wechat_product_tag.txt", sep="\t").fillna("")

product = product.set_index("b.user_id").to_dict("index")
product_map = copy.copy(product)
for k, v in product.iteritems():
    product_map[k] = dicttolsit(v)

# print product_map

##################################  attention_data

product_brand = pd.read_csv("/Users/guoyuanpei/workspace/pworkspace/mpintranettool/scripts/kol/weixin/wechat_brand_tag.txt", sep="\t").fillna("")

product_brand = product_brand.set_index("b.user_id").to_dict("split")

product_brand_map = dict(zip(product_brand["index"], product_brand["data"]))
product_brand_title = product_brand["columns"]


###################################   preference_data

preference_data = pd.read_csv("/Users/guoyuanpei/workspace/pworkspace/mpintranettool/scripts/kol/weixin/preference_data.txt", sep="\t").fillna("")

preference_data_map = dict()
for ix, row in preference_data.iterrows():
    user_id = row["account_id"]
    data = {"name": row["keywords"], "value": row["value"]}
    if preference_data_map.has_key(user_id):
        preference_data_map[user_id].append(data)
    else:
        preference_data_map[user_id] = [data]

# print preference_data_map

################################## post_data

post_data = pd.read_csv("/Users/guoyuanpei/workspace/pworkspace/mpintranettool/scripts/kol/weixin/post_data.csv", sep=",").fillna("")

post_data = post_data.set_index("id").to_dict("split")
post_data_map = dict(zip(post_data["index"], post_data["data"]))
post_data_title = post_data["columns"]

################################## star_data

star_data = pd.read_csv("/Users/guoyuanpei/workspace/pworkspace/mpintranettool/scripts/kol/weixin/star_data.csv", sep=",").fillna("")

star_data = star_data.set_index("id").to_dict("split")
star_data_map = dict(zip(star_data["index"], star_data["data"]))
for k, row in star_data_map.iteritems():
    star_data_map[k] = dict(total=row[0:5], avg=row[5:])

# print star_data_map

################################## yuanchuang.csv

reply_data = pd.read_csv("/Users/guoyuanpei/workspace/pworkspace/mpintranettool/scripts/kol/weixin/yuanchuang.csv", sep=",").fillna("")

reply_data = reply_data.set_index("id").to_dict("split")
reply_data_map = dict(zip(reply_data["index"], reply_data["data"]))
for k, row in reply_data_map.iteritems():
    reply_data_map[k] = dict(total=row[0:5], avg=row[5:])

# print reply_data_map

######################################################################################################


user_liebiao = pd.read_csv("/Users/guoyuanpei/workspace/pworkspace/mpintranettool/scripts/kol/weixin/kol_candidate_info.txt", sep="\t").fillna("")
user_liebiao["tmp"] = user_liebiao["id"]
user_liebiao = user_liebiao.set_index("tmp").to_dict("index")

user_list = copy.copy(user_liebiao)
for k, v in user_liebiao.iteritems():
    if product_map.has_key(str(k)):
        user_list[k]["attention_data"] = product_map[str(k)]

    if product_brand_map.has_key(str(k)):
        user_list[k]["product_brand"] = product_brand_map[str(k)]
        user_list[k]["product_brand_title"] = product_brand_title

    if preference_data_map.has_key(str(k)):
        user_list[k]["preference_data"] = preference_data_map[str(k)]

    if post_data_map.has_key(k):
        user_list[k]["post_data"] = post_data_map[k]
        user_list[k]["post_data_title"] = post_data_title

    if star_data_map.has_key(k):
        user_list[k]["star_data"] = star_data_map[k]

    if reply_data_map.has_key(k):
        user_list[k]["reply_data"] = reply_data_map[k]

# print output_json.values()

f = open("weixin.json", "wb")

data_str = json.dumps(user_list.values()[0:5], ensure_ascii=False)
f.write(data_str)
f.close()

