# coding: utf-8
from __future__ import unicode_literals

import sys
reload(sys)
sys.setdefaultencoding("utf-8")

import traceback

categoryname, itemid, itemname, itemurl, mainpicurl, itemattrdesc, listprice, discountprice, salesqty, salesamt, totalorders, platfrom, sku, skuinfo, last_sku, daterange = [''] * 16


def export_data(error_info):
    '''
    export data to hive client
    :param SKUList:
    :param error_info:
    :return:
    '''
    print "\t".join([unicode(daterange), unicode(categoryname), unicode(itemid), unicode(itemname), unicode(itemurl), unicode(mainpicurl), unicode(itemattrdesc), unicode(listprice), unicode(discountprice), unicode(salesqty), unicode(salesamt), unicode(totalorders), unicode(platfrom), unicode(sku), unicode(skuinfo), unicode(error_info)])


def get_sku_map(skustr):
    '''
    sku str 转化为sku map
    :param skustr:
    :return:
    '''
    if not skustr:
        return {}

    return {_.split(";")[0]: _ for _ in skustr.split(":") if len(_.split(";")) == 4}


def get_stock(today, last, salesqty):
    '''
    计算 库变 和 销售额
    :param today: 今日 sku str
    :param last: 最近一次 sku str
    :return: 添加 库变 和销售额的 值
    '''
    today = get_sku_map(today)
    last = get_sku_map(last)
    if not today:
        return ''

    if not last:
        return ':'.join(['{0};0;0;0;0'.format(_) for _ in today.values()])

    stock_all, stock_ct, negative_count, positive_sum = [0] * 4
    for skuid in today.keys():
        if not last.has_key(skuid):
            today[skuid] = '{0};0;0'.format(today[skuid])
            continue

        today_list = today[skuid].split(";")
        last_list = last[skuid].split(";")
        stock = int(last_list[3]) - int(today_list[3])
        stock_ct += 1
        if stock < 0:
            negative_count += 1

        if stock > 0:
            positive_sum += stock
            stock_all += stock

        today[skuid] = '{0};{1};{2}'.format(today[skuid], stock, stock * float(today_list[2]))

    skus = today.values()

    if not skus:
        return ""

    new_skus = []
    if stock_all > 3 * salesqty:
        new_skus = ["{0};{1};{2}".format(_, round(salesqty * 1.0 / stock_ct), round(salesqty * 1.0 / stock_ct) * float(_.split(";")[2])) for _ in skus]

    elif negative_count == 0:
        new_skus = ["{0};{1};{2}".format(_, _.split(";")[4], _.split(";")[5]) for _ in skus]

    elif negative_count > 0:
        negative_qty = 0
        if salesqty > positive_sum:
            negative_qty = round((salesqty - positive_sum) / negative_count)

        for sku in skus:
            sku_array = sku.split(";")
            sku_qty = negative_qty if int(sku_array[4]) < 0 else int(sku_array[4])
            new_skus.append("{0};{1};{2}".format(sku, sku_qty, sku_qty * float(sku_array[2])))

    return ':'.join(new_skus)


'''
main function
'''
for line in sys.stdin:
    try:
        categoryname, itemid, itemname, itemurl, mainpicurl, itemattrdesc, listprice, discountprice, salesqty, salesamt, totalorders, platfrom, sku, skuinfo, last_sku, daterange = [_.replace("\\N", "") for _ in line.strip().split('\t')]

        error_info = ""
        sku = get_stock(sku, last_sku, int(salesqty))
        export_data('')

    except Exception, e:
        erro_info = traceback.format_exc().decode().replace("\t", "  ").replace("\n", "  ;;;;")
        export_data(erro_info)



