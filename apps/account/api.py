# coding: utf-8
# __author__: ""
from __future__ import unicode_literals
from models import Menu
import json


def get_menus(request):
    """
    生成树形结构
    TODO: 用递归或者伪递归的方式改善这种硬编码的树生成
    :param request:
    :return:
    """
    menus = request.session.get("menus")
    if not menus:
        menu_list = get_menu_list(request)
        menus = [_ for _ in menu_list if not _["pid"]]
        for second in menus:
            second["menus"] = [_ for _ in menu_list if _["pid"] == second["id"]]
    return menus


def get_menu_list(request):
    """
    获取当前用户可操作的菜单
    :param request:
    :return:
    """
    menu_list = request.session.get("menu_list")

    if not menu_list:
        menu_query_set = Menu.objects.filter(is_enabled=True, is_visible=True).values("id", "name", "url", "pid", "seq")
        menu_list = list(menu_query_set.order_by("id", "seq"))
    return menu_list


def get_menu_id(request):
    """
    获取一级菜单ID
    :param request:
    :return:
    """
    menu_id = request.session.get("menu_id")

    if not menu_id:
        menu_id = 2000
        request.session["menu_id"] = menu_id
    # print menu_id
    return menu_id


def get_menu_name(request):
    """
    获取一级菜单名
    :param request:
    :return:
    """
    menu_list = get_menu_list(request)
    menus = [_ for _ in menu_list if _["url"] == request.path]
    return menus[0]["name"] if len(menus) > 0 else None


def check_menu(request):
    """
    检查当前用户是否拥有该菜单权限
    :param request:
    :return:
    """
    menu_urls = Menu.objects.filter(is_enabled=True, url__startswith=request.path).values_list("url", flat=True)

    return len(menu_urls) > 0
