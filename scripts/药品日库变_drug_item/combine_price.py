# coding: utf-8
from __future__ import unicode_literals

import sys
reload(sys)
sys.setdefaultencoding("utf-8")
import json
import traceback
import numpy as np
# import datetime
# from dateutil.relativedelta import relativedelta


DateRange, ItemID, ItemName, ItemAttrDesc, ShopId, ShopName, PlatformId, CategoryId, SKUList, price_list, MonthlySalesQty, DiscountPrice, PlatformId = list(np.random.choice([""], size=13))


def export_data(SKUList, error_info):
    '''
    export data to hive client
    :param SKUList:
    :param error_info:
    :return:
    '''
    print "\t".join([unicode(DateRange), unicode(ItemID), unicode(ItemName), unicode(ItemAttrDesc), unicode(ShopId), unicode(ShopName), unicode(PlatformId), unicode(CategoryId), unicode(SKUList), unicode(MonthlySalesQty), unicode(DiscountPrice), unicode(error_info)])


def taobo_price(sku_list_map, sku_price_map):
    '''
    combine price
    :param sku_list_map:
    :param sku_price_map:
    :return:
    '''
    sku_list = []

    for key, value in sku_list_map.iteritems():
        if sku_price_map.has_key(key):

            discount_price = sku_price_map[key]["discount_price"]
            if not discount_price.replace(".", "").isdigit():
                discount_price = value["price"]

            sku_list.append("=".join([unicode(key), unicode(discount_price), unicode(sku_price_map[key]["stock"])]))

    return sku_list


def tianmao_price(sku_list_map, sku_price_map):
    '''
    combine price
    :param sku_list_map:
    :param sku_price_map:
    :return:
    '''
    sku_list = []
    for key, value in sku_list_map.iteritems():
        skuId = value.pop("skuId")
        if sku_price_map.has_key(skuId):

            discount_price = sku_price_map[skuId]["discount_price"]
            if discount_price == "":
                discount_price = sku_price_map[skuId]["list_price"]

            sku_list.append("=".join([unicode(skuId), unicode(discount_price), unicode(value["stock"])]))

    return sku_list


def get_suk_price_map(sku_price_json):
    '''
    create sku price map
    :param sku_price_json:
    :return:
    '''
    sku_price_map = {}
    for _ in sku_price_json:
        sku_data = dict(list_price=_["list_price"], discount_price=_["discount_price"])
        if _.has_key("stock"):
            sku_data.update({"stock": _["stock"]})

        sku_price_map.update({_["sku_id"]: sku_data})

    return sku_price_map


def get_sku_price_str(sku_price_list):
    '''
    没有sku List 进行price list 计算
    :param sku_price_map:
    :return:
    '''
    sku_list = []
    for row in sku_price_list:
        discount_price = row["discount_price"]
        try:
            float(discount_price)
        except:
            discount_price = row["list_price"]
        sku_list.append("=".join([unicode(row["sku_id"]), unicode(discount_price), ""]))

    return sku_list

'''
main function
'''
for line in sys.stdin:
    try:
        DateRange, ItemID, ItemName, ItemAttrDesc, ShopId, ShopName, PlatformId, CategoryId, SKUList, price_list, MonthlySalesQty, DiscountPrice, PlatformId = [_.replace("\\N", "") for _ in line.strip().split('\t')]

        # if DateRange:
        #     dr = datetime.datetime.strptime(DateRange, "%Y-%m-%d") + relativedelta(days=-1)
        #     DateRange = dr.strftime("%Y-%m-%d")

        error_info = ""
        sku_list_map = {}

        sku_price_json = json.loads(price_list)
        sku_price_map = get_suk_price_map(sku_price_json["prices"])

        if not SKUList or SKUList == '""':
            SKUList = get_sku_price_str(sku_price_json["prices"])
            export_data("&&".join(SKUList), error_info)
            continue

        sku_list_map = json.loads(SKUList).get("skuMap")

        if PlatformId in ["7001", "7002"]:
            SKUList = taobo_price(sku_list_map, sku_price_map)

        elif PlatformId in ["7011", "7012"]:
            SKUList = tianmao_price(sku_list_map, sku_price_map)

        else:
            error_info = "1"

        export_data("&&".join(SKUList), error_info)

    except Exception, e:
        erro_info = traceback.format_exc().decode().replace("\t", "  ").replace("\n", "  ;;;;")
        export_data("", erro_info)



