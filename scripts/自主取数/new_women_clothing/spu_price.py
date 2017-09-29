# coding: utf-8
from __future__ import unicode_literals

import sys
reload(sys)
sys.setdefaultencoding("utf-8")
import traceback
import numpy as np


ItemID, SKUList, sku_day_salesqty, sku_day_salesamt, sku_months_salesqty, sku_months_salesamt, sku_day_stock_change, sku_months_stock_change, SalesQty, listeddate, is_has_months_change, DateRange = list(np.random.choice([""], size=12))


def export_data(spu_price, error_info):
    '''
    export data to hive client
    :param spu_price:
    :param error_info:
    :return:
    '''
    print "\t".join([unicode(DateRange), unicode(ItemID), unicode(sku_day_salesqty), unicode(sku_day_salesamt), unicode(sku_day_stock_change), unicode(spu_price), unicode(SKUList), unicode(SalesQty), unicode(listeddate), unicode(error_info)])


def sku_list_to_sku_map(skulist):
    '''
    skulist str to skulist
    :param skulist:
    :return:
    '''
    skuMap = {}
    for sku in skulist.split("&&"):
        sku_array = sku.split("=")
        if len(sku_array) == 3:
            skuMap.update({sku_array[0]: dict(discount_price=sku_array[1], stock=sku_array[2])})

    return skuMap


def get_avg_sku_price(SKUList):
    '''
    获取 sku 的平均折扣价, 所有价格是相同
    :param SKUList:
    :return:
    '''
    is_unique_price = set()
    price_all = 0
    sku_list = SKUList.split("&&")

    for sku in sku_list:
        price = float(sku.split("=")[1])

        is_unique_price.add(price)
        price_all += price

    return price_all / len(sku_list), 0 if len(is_unique_price) > 1 else 1


def get_avg_day_stock_change_price(sku_day_stock_change):
    '''
    获取日库变 加权平均价
    :param sku_day_stock_change:
    :return:
    '''
    price_all = 0
    stock_all = 0
    sku_list = sku_day_stock_change.split("&&")
    for sku in sku_list:
        sku_data = sku.split("=")
        stock = int(sku_data[2])
        discount_price = float(sku_data[1])

        stock_all += stock
        price_all += discount_price * stock

    if stock_all == 0:
        return 0

    return price_all / stock_all


def months_stock_change_is_zero(sku_months_stock_change):
    '''
    获取 sku 月库变 是否都等于 0
    :param sku_months_stock_change:
    :return:
    '''
    is_zero = True
    for sku in sku_months_stock_change.split("&&"):

        sku_data = sku.split("=")
        stock = int(sku_data[1])
        if stock != 0:
            is_zero = False
            break

    return is_zero


def get_avg_months_stock_change_price(SKUList, sku_months_stock_change):
    '''
    获取 月库变 加权平均价
    :param SKUList:
    :param sku_months_stock_change:
    :return:
    '''
    sku_map = sku_list_to_sku_map(SKUList)

    sum_amt = 0
    sum_stock = 0
    for sku in sku_months_stock_change.split("&&"):
        sku_data = sku.split("=")
        sku_key = sku_data[0]
        if sku_map.has_key(sku_key):
            price = float(sku_map[sku_key].get("discount_price"))
            stock = int(sku_data[1])

            sum_amt += price * stock
            sum_stock += stock

    if sum_stock == 0:
        return 0.0

    return sum_amt / sum_stock


def get_spu_price(SKUList, sku_months_salesqty, sku_months_salesamt, sku_months_stock_change, is_has_months_change):
    '''
    获取 spu 价格
    :param SKUList: 当日 sku list
    :param sku_months_salesqty: 月 sku销量
    :param sku_months_salesamt: 月 sku销售额
    :param sku_months_stock_change: sku 月库变
    :param salesqty: 日销量
    :param is_has_months_change: 改商品 的是否有月 库变。 即 第一次出现的 时间是否在一个月之外
    :return:
    '''
    if SKUList == "":
        return 0

    avg_price, is_price_unique = get_avg_sku_price(SKUList)  # 1

    if is_price_unique == 1:
        return avg_price

    if is_has_months_change != "1":  # 4
        if sku_months_salesqty == "0":
            return avg_price
        return float(sku_months_salesamt) / float(sku_months_salesqty)

    if months_stock_change_is_zero(sku_months_stock_change):  # 1
        return avg_price

    return get_avg_months_stock_change_price(SKUList, sku_months_stock_change)  # 3


'''
main function
'''
for line in sys.stdin:
    try:
        ItemID, SKUList, sku_day_salesqty, sku_day_salesamt, sku_months_salesqty, sku_months_salesamt,\
        sku_day_stock_change, sku_months_stock_change, SalesQty, listeddate, is_has_months_change, DateRange = [_.replace("\\N", "") for _ in line.strip().split('\t')]

        spu_price = get_spu_price(SKUList, sku_months_salesqty, sku_months_salesamt, sku_months_stock_change, is_has_months_change)
        export_data(spu_price, "")

    except Exception, e:
        erro_info = traceback.format_exc().decode().replace("\t", "  ").replace("\n", "  ;;;;")
        export_data("", erro_info)



