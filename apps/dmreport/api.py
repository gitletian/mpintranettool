# coding: utf-8
# __author__: ""
from __future__ import unicode_literals

from tools.db.hive import Hive
from apps.dmreport.models import Category
import sql
import datetime
from dateutil.relativedelta import relativedelta
import itertools
import pandas as pd
from models import Shop
import re
import pdb


def get_data_source():
    data_source = list([
        dict(name="卖家网与生意参谋", id="1"),
        dict(name="自主取数与生意参谋", id="2"),
    ])
    return data_source


def is_digit(num):
    '''
    判断字符串是否是 float 类型
    :param num:
    :return:
    '''
    try:
        float(num)
        return True
    except Exception,e:
        return False


def get_category_list():
    '''
    获取 category list
    :return:
    '''
    category_list = list(Category.objects.values("id", "pid", "name").filter(industry=16))
    return category_list


def get_category(category_id):
    '''
    根据id 或取 category
    :param category_id:
    :return:
    '''
    category = Category.objects.get(pk=category_id)
    return category


def get_compare_table(data_source):
    '''
    获取 数据源
    :param data_source:
    :return:
    '''
    compare_table = {"1": "elengjing.women_clothing_item", "2": "mpintranet.day_qut"}
    return compare_table.get(unicode(data_source), "elengjing.women_clothing_item")


def item_month_ragezb_data(param):
    '''
    商品层级
        获取 月度 数据占比
    :param param:
    :return:
    '''
    hsql = sql.ITEM_MONTH_RAGEZB.format(**param)
    print hsql
    data_list = Hive().query(hsql)

    return data_list


def item_day_ragezb_data(param):
    '''
    商品层级
        获取 日度 数据占比
    :param param:
    :return:
    '''
    hsql = sql.ITEM_DAY_RAGEZB.format(**param)
    print hsql
    data_list = Hive().query(hsql)

    return data_list


def item_month_rage_data(param):
    '''
    商品层级
        获取 月度 数据 图
    :param param:
    :return:
    '''
    hsql = sql.ITEM_MONTH_RAGE.format(**param)
    print hsql
    data_list = Hive().query(hsql)
    return data2echarts(data_list)


def item_day_rage_data(param):
    '''
    商品层级
        获取 日度 数据 图
    :param param:
    :return:
    '''
    hsql = sql.ITEM_DAY_RAGE.format(**param)
    print hsql
    data_list = Hive().query(hsql)

    return data2echarts(data_list)


def data2echarts(datalist):
    '''
    将数据 封装成 echar 数据
    :param datalist:
    :return:
    '''
    data_map = {}
    data_range_list = set()
    shop_list = set()
    for row in datalist:
        data_range_list.add(row["daterange"])
        shop_list.add(row["shopname"])
        if data_map.has_key(row["itemid"]):
            item = data_map[row["itemid"]]
            if item.has_key(row["shopname"]):
                shop = item[row["shopname"]]
                shop[row["daterange"]] = dict(
                        av_price_disc_rate=row["av_price_disc_rate"],
                        salesmmt_disc_rate=row["salesmmt_disc_rate"],
                        salesqty_disc_rate=row["salesqty_disc_rate"],
                    )
            else:
                item[row["shopname"]] = {row["daterange"]: dict(
                        av_price_disc_rate=row["av_price_disc_rate"],
                        salesmmt_disc_rate=row["salesmmt_disc_rate"],
                        salesqty_disc_rate=row["salesqty_disc_rate"],
                    )}
        else:
            data_map[row["itemid"]] = {row["shopname"]: {row["daterange"]: dict(
                        av_price_disc_rate=row["av_price_disc_rate"],
                        salesmmt_disc_rate=row["salesmmt_disc_rate"],
                        salesqty_disc_rate=row["salesqty_disc_rate"],
                    )}}
    data_range_list = list(data_range_list)
    shop_list = list(shop_list)
    data_range_list.sort()
    for itemid, shops in data_map.iteritems():
        for shopanme in shop_list:
            if not shops.has_key(shopanme):
                shops[shopanme] = {}

        for shopname, shop in shops.iteritems():
            for data_range in data_range_list:
                if not shop.has_key(data_range):
                    shop[data_range] = dict(
                        av_price_disc_rate=0,
                        salesmmt_disc_rate=0,
                        salesqty_disc_rate=0,
                    )
    data_map["data_range_list"] = data_range_list
    data_map["shop_list"] = shop_list
    return data_map


def shop_month_ragezb_data(param):
    '''
    店铺层级
        获取月数据占比
    :param param:
    :return:
    '''
    hsql = sql.SHOP_MONTH_RAGEZB.format(**param)
    print hsql
    data_list = Hive().query(hsql)

    return data_list


def shop_day_ragezb_data(param):
    '''
    店铺层级
        获取日数据占比
    :param param:
    :return:
    '''
    hsql = sql.SHOP_DAY_RAGEZB.format(**param)
    print hsql
    data_list = Hive().query(hsql)

    return data_list


def shop_month_rage_data(param, daterange_list):
    '''
    店铺层级
        获取 月 数据 图
    :param param:
    :param daterange_list:
    :return:
    '''
    hsql = sql.SHOP_MONTH_RAGE.format(**param)
    print hsql
    data_list, columns = Hive().query(hsql, meta=True)

    shopids = param["shop_ids"].split(",")
    all_data_df = get_all_data(data_list, columns, daterange_list, shopids)

    zhibiaos = [("av_price_disc_rate", "平均单价差异率"), ("spu_disc_rate", "商品数差异率")
        , ("salesqty_disc_rate", "销售量差异率"), ("salesmmt_disc_rate", "销售额差异率")]
    day_rage_data = df2echar(all_data_df, daterange_list, zhibiaos)
    return day_rage_data


def shop_day_rage_data(param, daterange_list):
    '''
    店铺层级
        获取 日 数据 图
    :param param:
    :param daterange_list:
    :return:
    '''
    hsql = sql.SHOP_DAY_RAGE.format(**param)
    print hsql
    data_list, columns = Hive().query(hsql, meta=True)

    shopids = param["shop_ids"].split(",")
    all_data_df = get_all_data(data_list, columns, daterange_list, shopids)

    zhibiaos = [("av_price_disc_rate", "平均单价差异率"), ("spu_disc_rate", "商品数差异率")
        , ("salesqty_disc_rate", "销售量差异率"), ("salesmmt_disc_rate", "销售额差异率")]
    day_rage_data = df2echar(all_data_df, daterange_list, zhibiaos)

    return day_rage_data


def df2echar(df, daterange_list, zhibiaos):
    '''
    DataFrame 转换成 Echer 格式
    :param df:
    :param daterange_list:
    :param zhibiaos:
    :return:
    '''
    shopname_list = df.shopname.unique().tolist()

    zhibiao_data_all = []
    for zhibiao in zhibiaos:
        series_list = []
        for shopname in shopname_list:
            series_data = df[df.shopname == shopname][zhibiao[0]].tolist()
            series_list.append({"shopname": shopname, "data": series_data})

        zhibiao_data_all.append(dict(series_list=series_list, title=zhibiao[1]))

    return dict(legend_data=shopname_list, xAxis_data=daterange_list, zhibiao_data_all=zhibiao_data_all)


def get_all_data(data_list, columns, daterange, shop_ids):
    '''
    获取shop_ids 和 daterange 的交叉数据集,默认填空为 0
    :param data_list: 数据集
    :param daterange: daterange list
    :param shop_ids: shop_id list
    :return: DataFrame
    '''
    shop_list = list(Shop.objects.filter(pk__in=shop_ids).values("id", "name"))
    shop_info = pd.DataFrame(shop_list)
    shop_info.columns = ["shopid", "shopname"]

    index_df = pd.DataFrame(list(itertools.product(shop_ids, daterange)), columns=["shopid", "daterange"])
    index_df["shopid"] = index_df["shopid"].astype("int")

    data_df = pd.DataFrame(data_list, columns=columns)

    all_data_df = index_df.merge(shop_info, how="left", on="shopid")\
        .merge(data_df, how="left", on=["shopid", "daterange"]).fillna(0)

    return all_data_df


def get_daterange_list(min_date, max_date, type="months"):
    '''
    获取时间差 list
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


def category_df2echar(all_data_df, shop_list, categoryid_list, zhibiaos, daterange_list):
    '''
    品类层级
        df 数据 转 echarts 格式
    :param all_data_df:
    :param shop_list:
    :param categoryid_list:
    :param zhibiaos:
    :param daterange_list:
    :return:
    '''
    shop_data_list = []
    for shop in shop_list:
        s_df = all_data_df[all_data_df.shopid == shop["id"]]
        zhibiao_data_all = []
        for zhibiao in zhibiaos:
            series_list = []
            for category in categoryid_list:
                series_data = s_df[s_df.categoryid == category["id"]][zhibiao[0]].tolist()
                series_list.append({"categoryid": category["id"], "data": series_data})

            zhibiao_data_all.append(dict(series_list=series_list, title=zhibiao[1]))

        shop_data_list.append(dict(zhibiao_data_all=zhibiao_data_all, shopid=shop["id"], shopname=shop["name"]))
    return dict(legend_data=[_["id"] for _ in categoryid_list], xAxis_data=daterange_list, shop_data_list=shop_data_list)


def category_all_data_df(data_list, columns, category_list, daterange, shop_list):
    '''
    品类层级
        获取品类差异的全 数据 df
    :param data_list:
    :param columns:
    :param daterange:
    :param shop_ids:
    :return:
    '''
    shop_info = pd.DataFrame(shop_list)
    shop_info.columns = ["shopid", "shopname"]

    data_df = pd.DataFrame(data_list, columns=columns)

    index_pf = pd.DataFrame(list(itertools.product([_["id"] for _ in shop_list], [_["id"] for _ in category_list], daterange)), columns=["shopid", "categoryid", "daterange"])
    index_pf["categoryid"] = index_pf.categoryid.astype("int")

    all_data_df = index_pf.merge(shop_info, how="left", on="shopid")\
        .merge(data_df, how="left", on=["shopid", "categoryid", "daterange"]).fillna(0)

    return all_data_df


def category_all_data_shop_df(data_list, columns, category_list, shop_list, daterange):
    '''
    品类层级
        获取品类差异的全 数据 df
    :param data_list:
    :param columns:
    :param daterange:
    :param shop_ids:
    :return:
    '''
    shop_info = pd.DataFrame(shop_list)
    shop_info.columns = ["shopid", "shopname"]

    data_df = pd.DataFrame(data_list, columns=columns)
    index_pf = pd.DataFrame(list(itertools.product([_["id"] for _ in category_list], [_["id"] for _ in shop_list], daterange)), columns=["categoryid", "shopid", "daterange"])
    # index_pf["shopid"] = index_pf.shopid.astype("int")

    all_data_df = index_pf.merge(shop_info, how="left", on="shopid")\
        .merge(data_df, how="left", on=["categoryid", "shopid", "daterange"]).fillna(0)

    return all_data_df


def category_shop_df2echar(df, category_list, zhibiaos, daterange_list):
    '''
    品类层级
        df 数据 转 echarts 格式
    :param all_data_df:
    :param shop_list:
    :param categoryid_list:
    :param zhibiaos:
    :param daterange_list:
    :return:
    '''
    shopname_list = df.shopname.unique().tolist()
    all_data = []
    for _ in category_list:
        zhibiao_data_all = []
        for zhibiao in zhibiaos:
            series_list = []
            for shopname in shopname_list:
                series_data = df[df.shopname == shopname][zhibiao[0]].tolist()
                series_list.append({"shopname": shopname, "data": series_data})

            zhibiao_data_all.append(dict(series_list=series_list, title=zhibiao[1]))

        all_data.append(dict(categoryid=_["id"], categoryname=_["name"], zhibiao_data_all=zhibiao_data_all))

    return dict(legend_data=shopname_list, xAxis_data=daterange_list, all_data=all_data)


def category_month_rage_data(param, daterange_list, category_list):
    '''
    品类层级
        获取品类月差异率曲线
    :param param:
    :param daterange_list:
    :return:
    '''
    hsql = sql.CATEGORY_MONTH_RAGE.format(**param)
    print hsql
    data_list, columns = Hive().query(hsql, meta=True)

    shopids = param["shop_ids"].split(",")
    shop_list = list(Shop.objects.filter(pk__in=shopids).values("id", "name"))
    all_data_df = category_all_data_df(data_list, columns, category_list, daterange_list, shop_list)

    zhibiaos = [("av_price_disc_rate", "平均单价差异率"), ("spu_disc_rate", "商品数差异率")
        , ("salesqty_disc_rate", "销售量差异率"), ("salesmmt_disc_rate", "销售额差异率")]

    month_rage_data = category_df2echar(all_data_df, shop_list, category_list, zhibiaos, daterange_list)
    return month_rage_data


def category_month_rage_shop_data(param, daterange_list, category_list):
    '''
    品类层级
        获取店铺月差异率曲线
    :param param:
    :param daterange_list:
    :return:
    '''
    hsql = sql.CATEGORY_MONTH_RAGE_SHOP.format(**param)
    print hsql
    data_list, columns = Hive().query(hsql, meta=True)

    shopids = param["shop_ids"].split(",")
    shop_list = list(Shop.objects.filter(pk__in=shopids).values("id", "name"))

    all_data_df = category_all_data_shop_df(data_list, columns, category_list, shop_list, daterange_list)

    zhibiaos = [("av_price_disc_rate", "平均单价差异率"), ("spu_disc_rate", "商品数差异率")
        , ("salesqty_disc_rate", "销售量差异率"), ("salesmmt_disc_rate", "销售额差异率")]

    day_rage_data = category_shop_df2echar(all_data_df, category_list, zhibiaos, daterange_list)
    return day_rage_data


def category_day_rage_data(param, daterange_list, category_list):
    '''
    品类层级
        获取品类日差异率曲线
    :param param:
    :param daterange_list:
    :return:
    '''
    hsql = sql.CATEGORY_DAY_RAGE.format(**param)
    print hsql
    data_list, columns = Hive().query(hsql, meta=True)

    shopids = param["shop_ids"].split(",")
    shop_list = list(Shop.objects.filter(pk__in=shopids).values("id", "name"))
    all_data_df = category_all_data_df(data_list, columns, category_list, daterange_list, shop_list)

    zhibiaos = [("av_price_disc_rate", "平均单价差异率"), ("spu_disc_rate", "商品数差异率")
        , ("salesqty_disc_rate", "销售量差异率"), ("salesmmt_disc_rate", "销售额差异率")]

    month_rage_data = category_df2echar(all_data_df, shop_list, category_list, zhibiaos, daterange_list)
    return month_rage_data


def category_day_rage_shop_data(param, daterange_list, category_list):
    '''
    品类层级
        获取店铺日差异率曲线
    :param param:
    :param daterange_list:
    :return:
    '''
    hsql = sql.CATEGORY_DAY_RAGE_SHOP.format(**param)
    print hsql
    data_list, columns = Hive().query(hsql, meta=True)

    shopids = param["shop_ids"].split(",")
    shop_list = list(Shop.objects.filter(pk__in=shopids).values("id", "name"))

    all_data_df = category_all_data_shop_df(data_list, columns, category_list, shop_list, daterange_list)

    zhibiaos = [("av_price_disc_rate", "平均单价差异率"), ("spu_disc_rate", "商品数差异率")
        , ("salesqty_disc_rate", "销售量差异率"), ("salesmmt_disc_rate", "销售额差异率")]

    day_rage_data = category_shop_df2echar(all_data_df, category_list, zhibiaos, daterange_list)
    return day_rage_data




