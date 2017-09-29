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

product = pd.read_csv(
        "/Users/guoyuanpei/workspace/pworkspace/mpintranettool/scripts/kol/attention_data_candidate_weibotagProduct.txt", sep="\t").fillna("")

product = product.set_index("user_id").to_dict("index")
product_map = copy.copy(product)
for k, v in product.iteritems():
    product_map[k] = dicttolsit(v)

# print product_map


##################################  attention_data

product_brand = pd.read_csv(
        "/Users/guoyuanpei/workspace/pworkspace/mpintranettool/scripts/kol/attention_data_candidate_weibotagBrand.txt", sep="\t").fillna("")

product_brand = product_brand.set_index("user_id").to_dict("split")

product_brand_map = dict(zip(product_brand["index"], product_brand["data"]))
product_brand_title = product_brand["columns"]




###################################   preference_data

preference_data = pd.read_csv("/Users/guoyuanpei/workspace/pworkspace/mpintranettool/scripts/kol/preference_data.txt", sep="\t").fillna("")

preference_data_map = dict()
for ix, row in preference_data.iterrows():
    user_id = row["user_id"]
    data = {"name": row["keywords"], "value": row["value"]}
    if preference_data_map.has_key(user_id):
        preference_data_map[user_id].append(data)
    else:
        preference_data_map[user_id] = [data]

# print preference_data_map

################################## post_data

post_data = pd.read_csv("/Users/guoyuanpei/workspace/pworkspace/mpintranettool/scripts/kol/post_data.txt", sep="\t").fillna("")

post_data = post_data.set_index("user_id").to_dict("split")
post_data_map = dict(zip(post_data["index"], post_data["data"]))
post_data_title = post_data["columns"]

# print post_data_map

################################## repost_data

repost_data = pd.read_csv("/Users/guoyuanpei/workspace/pworkspace/mpintranettool/scripts/kol/repost_data.txt", sep="\t").fillna("")

repost_data = repost_data.set_index("user_id").to_dict("split")
repost_data_map = dict(zip(repost_data["index"], repost_data["data"]))
for k, row in repost_data_map.iteritems():
    repost_data_map[k] = dict(total=row[0:5], avg=row[5:])

# print post_data_map


################################## star_data

star_data = pd.read_csv("/Users/guoyuanpei/workspace/pworkspace/mpintranettool/scripts/kol/star_data.txt", sep="\t").fillna("")

star_data = star_data.set_index("user_id").to_dict("split")
star_data_map = dict(zip(star_data["index"], star_data["data"]))
for k, row in star_data_map.iteritems():
    star_data_map[k] = dict(total=row[0:5], avg=row[5:])

# print star_data_map

################################## reply_data

reply_data = pd.read_csv("/Users/guoyuanpei/workspace/pworkspace/mpintranettool/scripts/kol/reply_data.txt", sep="\t").fillna("")

reply_data = reply_data.set_index("user_id").to_dict("split")
reply_data_map = dict(zip(reply_data["index"], reply_data["data"]))
for k, row in reply_data_map.iteritems():
    reply_data_map[k] = dict(total=row[0:5], avg=row[5:])

# print reply_data_map

################################## fans_gestation_data

fans_gestation = pd.read_csv(
        "/Users/guoyuanpei/workspace/pworkspace/mpintranettool/scripts/kol/fans_gestation_data.txt", sep="\t").fillna("")

fans_gestation = fans_gestation.set_index("user_id").to_dict("index")
fans_gestation_map = copy.copy(fans_gestation)
for k, v in fans_gestation.iteritems():
    fans_gestation_map[k] = dicttolsit(v)

# print fans_gestation_map

################################## fans_city_data

fans_city = pd.read_csv(
        "/Users/guoyuanpei/workspace/pworkspace/mpintranettool/scripts/kol/fans_city_data.txt", sep="\t").fillna("")

fans_city = fans_city.set_index("user_id").to_dict("index")
fans_city_map = copy.copy(fans_city)
for k, v in fans_city.iteritems():
    fans_city_map[k] = dicttolsit(v)

# print fans_city_map


######################################################################################################


user_liebiao = pd.read_csv("/Users/guoyuanpei/workspace/pworkspace/mpintranettool/scripts/kol/user_liebiao.txt", sep="\t").fillna("")
user_liebiao["tmp"] = user_liebiao["user_id"]
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

    if repost_data_map.has_key(k):
        user_list[k]["repost_data"] = repost_data_map[k]

    if star_data_map.has_key(k):
        user_list[k]["star_data"] = star_data_map[k]

    if reply_data_map.has_key(k):
        user_list[k]["reply_data"] = reply_data_map[k]

    if fans_gestation_map.has_key(k):
        user_list[k]["fans_gestation_data"] = fans_gestation_map[k]

    if fans_city_map.has_key(k):
        user_list[k]["fans_city_data"] = fans_city_map[k]

# print output_json.values()

f = open("weibo.json", "wb")

data_str = json.dumps(sorted(user_list.values(), key=lambda d: d["index"]), ensure_ascii=False)
f.write(data_str)
f.close()

