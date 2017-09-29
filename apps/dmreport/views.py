# coding: utf-8
# __author__: ""
from __future__ import unicode_literals

from django.http import HttpResponse, JsonResponse
from django.shortcuts import render
import api
import json
from models import Category
import pdb


def test(request):
    '''
    测试方法
    :param request:
    :return:
    '''
    result = {"status": 0, "message": ""}
    return HttpResponse(result)


def item_diff_main(request):
    '''
    商品层级主页面
    :param request:
    :return:
    '''
    data_source = api.get_data_source()
    category_list = api.get_category_list()

    return render(request, "dmreport/item_diff_main.html", dict(data_source=data_source, category_list=category_list))


def item_diff_content(request, template_name):
    '''
    商品层级
    :param request:
    :param template_name:
    :return:
    '''
    result = {'status': 0, 'message': ''}

    month_ragezb_data, day_ragezb_data, month_rage_data, day_rage_data = [], [], [], []
    param = json.loads(request.body)

    data_source = param.get("data_source")
    min_rate = param.get("min_rate")
    max_rate = param.get("max_rate")
    category_id = param.get("category_id")
    min_date = param.get("min_date")
    max_date = param.get("max_date")
    dates = param.get("dates")

    if not data_source:
        return JsonResponse({'status': 0, 'message': '参数错误'}, safe=False)

    if not api.is_digit(min_rate):
        return JsonResponse({'status': 0, 'message': '参数错误'}, safe=False)

    if not api.is_digit(max_rate):
        return JsonResponse({'status': 0, 'message': '参数错误'}, safe=False)

    if not category_id:
        return JsonResponse({'status': 0, 'message': '参数错误'}, safe=False)

    if (not min_date or not max_date) and not dates:
        return JsonResponse({'status': 0, 'message': '参数错误'}, safe=False)

    param["compare_table"] = api.get_compare_table(data_source)
    param["shop_ids"] = "69302618,66098091,73401272"
    param["min_rate"] = float(min_rate) / 100.0
    param["max_rate"] = float(max_rate) / 100.0

    if min_date and max_date:
        month_ragezb_data = api.item_month_ragezb_data(param)
        month_rage_data = api.item_month_rage_data(param)

    if dates:
        param["dates"] = "','".join(dates.split(","))
        day_ragezb_data = api.item_day_ragezb_data(param)
        day_rage_data = api.item_day_rage_data(param)

    category = api.get_category(category_id)
    data = dict(
        month_ragezb_data=month_ragezb_data,
        day_ragezb_data=day_ragezb_data,
        month_rage_data=json.dumps(month_rage_data),
        day_rage_data=json.dumps(day_rage_data),
        category_name=category.name,
    )

    response = render(request, template_name, data)

    result["status"] = 1
    result["data"] = response.content

    return JsonResponse(result, safe=False)


def shop_diff_main(request):
    '''
    获取店铺层级主页面
    :param request:
    :return:
    '''
    data_source = api.get_data_source()

    return render(request, "dmreport/shop_diff_main.html", dict(data_source=data_source))


def shop_diff_content(request, template_name):
    '''
    店铺层级
    :param request:
    :param template_name:
    :return:
    '''
    result = {'status': 0, 'message': ''}

    month_ragezb_data, day_ragezb_data, month_rage_data, day_rage_data = [], [], [], []
    param = json.loads(request.body)

    data_source = param.get("data_source")
    min_rate = param.get("min_rate")
    max_rate = param.get("max_rate")
    min_date = param.get("min_date")
    max_date = param.get("max_date")
    dates = param.get("dates")
    if not data_source:
        return JsonResponse({'status': 0, 'message': '参数错误'}, safe=False)

    if not api.is_digit(min_rate):
        return JsonResponse({'status': 0, 'message': '参数错误'}, safe=False)

    if not api.is_digit(max_rate):
        return JsonResponse({'status': 0, 'message': '参数错误'}, safe=False)

    if (not min_date or not max_date) and not dates:
        return JsonResponse({'status': 0, 'message': '参数错误'}, safe=False)

    param["compare_table"] = api.get_compare_table(data_source)
    param["shop_ids"] = "69302618,66098091,73401272"
    param["min_rate"] = float(min_rate) / 100.0
    param["max_rate"] = float(max_rate) / 100.0

    if min_date and max_date:
        daterange_list = api.get_daterange_list(min_date, max_date)
        param["date_len"] = len(daterange_list)

        month_ragezb_data = api.shop_month_ragezb_data(param)
        month_rage_data = api.shop_month_rage_data(param, daterange_list)

    if dates:
        daterange_list = dates.split(",")
        param["date_len"] = len(daterange_list)
        param["dates"] = "','".join(daterange_list)

        day_ragezb_data = api.shop_day_ragezb_data(param)
        day_rage_data = api.shop_day_rage_data(param, daterange_list)

    data = dict(
        month_ragezb_data=month_ragezb_data,
        day_ragezb_data=day_ragezb_data,
        month_rage_data=json.dumps(month_rage_data),
        day_rage_data=json.dumps(day_rage_data),
    )

    response = render(request, template_name, data)

    result["status"] = 1
    result["data"] = response.content

    return JsonResponse(result, safe=False)


def category_diff_main(request):
    '''
    获取品类层级主页面
    :param request:
    :return:
    '''
    data_source = api.get_data_source()
    category_list = api.get_category_list()

    return render(request, "dmreport/category_diff_main.html", dict(data_source=data_source, category_list=category_list))


def category_diff_content(request, template_name):
    '''
    品类层级
    :param request:
    :param template_name:
    :return:
    '''
    result = {'status': 0, 'message': ''}

    month_ragezb_data, day_ragezb_data, month_rage_data, day_rage_data = [], [], [], []
    param = json.loads(request.body)

    data_source = param.get("data_source", 1)
    min_qty = param["min_qty"] = param["min_qty"] if param.get("min_qty") else "40"
    min_rate = param.get("min_rate", -10)
    max_rate = param.get("max_rate", 10)
    category_id = param.get("category_id")
    min_date = param.get("min_date")
    max_date = param.get("max_date")
    dates = param.get("dates")

    if not data_source:
        return JsonResponse({'status': 0, 'message': '参数错误'}, safe=False)

    if not min_qty.isdigit():
        return JsonResponse({'status': 0, 'message': '参数错误'}, safe=False)

    if not api.is_digit(min_rate):
        return JsonResponse({'status': 0, 'message': '参数错误'}, safe=False)

    if not api.is_digit(max_rate):
        return JsonResponse({'status': 0, 'message': '参数错误'}, safe=False)

    if (not min_date or not max_date) and not dates:
        return JsonResponse({'status': 0, 'message': '参数错误'}, safe=False)

    if category_id:
        param["category_where"] = ' and categoryid = {0} '.format(category_id)
        category_list = [api.get_category(category_id).values("id", "name")]
    else:
        param["category_where"] = ""
        category_list = Category.objects.filter(pid=0, industry=16).values("id", "name")

    param["compare_table"] = api.get_compare_table(data_source)
    param["shop_ids"] = "69302618,66098091,73401272"
    param["min_rate"] = float(min_rate) / 100.0
    param["max_rate"] = float(max_rate) / 100.0

    if min_date and max_date:
        daterange_list = api.get_daterange_list(min_date, max_date)
        month_rage_data = api.category_month_rage_data(param, daterange_list, category_list)
        month_rage_shop_data = api.category_month_rage_shop_data(param, daterange_list, category_list)

    if dates:
        daterange_list = dates.split(",")
        param["dates"] = "','".join(daterange_list)

        day_rage_data = api.category_day_rage_data(param, daterange_list, category_list)
        day_rage_shop_data = api.category_day_rage_shop_data(param, daterange_list, category_list)

    data = dict(
        month_rage_data=json.dumps(month_rage_data),
        month_rage_shop_data=json.dumps(month_rage_shop_data),
        day_rage_data=json.dumps(day_rage_data),
        day_rage_shop_data=json.dumps(day_rage_shop_data),
        category_list=category_list,
    )

    response = render(request, template_name, data)

    result["status"] = 1
    result["data"] = response.content

    return JsonResponse(result, safe=False)

