# coding: utf-8
from __future__ import unicode_literals

import sys
reload(sys)
sys.setdefaultencoding("utf-8")
import traceback
import numpy as np

ItemID, p_SKUList, y_SKUList, m_DateRange, p_DateRange = list(np.random.choice([""], size=5))


def export_data(sku_day_salesqty, sku_day_salesamt, sku_day_stock_change, is_has_months_change, error_info):
    '''
    export data to hive client
    :param day_stock_change:
    :param day_salesamt:
    :param sku_day_stock_change:
    :param is_has_months_change:
    :param error_info:
    :return:
    '''
    print "\t".join([unicode(p_DateRange), unicode(ItemID), unicode(p_SKUList), unicode(sku_day_salesqty), unicode(sku_day_salesamt), unicode(sku_day_stock_change), unicode(is_has_months_change), unicode(error_info)])


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


def get_stock_change(p_SKUList, y_SKUList, m_DateRange):
    '''
    get stock change
    :param p_SKUList:
    :param y_SKUList:
    :param m_DateRange:
    :return:
    '''
    sku_day_stock_change_list = []
    sku_day_salesqty = 0
    sku_day_salesamt = 0.0
    is_has_months_change = 0

    if m_DateRange != "":
        is_has_months_change = 1

    if y_SKUList == "":
        return sku_day_salesqty, sku_day_salesamt, "", is_has_months_change

    p_sku_map = sku_list_to_sku_map(p_SKUList)
    y_sku_map = sku_list_to_sku_map(y_SKUList)

    for key, value in p_sku_map.iteritems():

        if key == '' or not y_sku_map.has_key(key):
            continue

        y_stock = y_sku_map[key].get("stock")
        p_stock = value.get("stock")
        discount_price = value.get("discount_price")

        if y_stock != "" and p_stock != "":
            stock_change = int(y_stock) - int(p_stock)

            if stock_change > 100:
                sku_day_salesqty = 0
                sku_day_salesamt = 0.0
                sku_day_stock_change_list = []
                break
            sku_day_salesqty += stock_change
            sku_day_salesamt += stock_change * float(discount_price)
            sku_day_stock_change_list.append("=".join([key, discount_price, unicode(stock_change)]))

    return sku_day_salesqty, sku_day_salesamt, "&&".join(sku_day_stock_change_list), is_has_months_change


'''
main function
'''
for line in sys.stdin:
    try:
        ItemID, p_SKUList, y_SKUList, m_DateRange, p_DateRange = [_.replace("\\N", "") for _ in line.strip().split('\t')]

        sku_day_salesqty, sku_day_salesamt, sku_day_stock_change, is_has_months_change = get_stock_change(p_SKUList, y_SKUList, m_DateRange)

        export_data(sku_day_salesqty, sku_day_salesamt, sku_day_stock_change, is_has_months_change, "")

    except Exception, e:
        erro_info = traceback.format_exc().decode().replace("\t", "  ").replace("\n", "  ;;;;")
        export_data("", "", "", "", erro_info)



