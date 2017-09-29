# coding: utf-8
# __author__: ""
from __future__ import unicode_literals

import json
import random


from django.shortcuts import render
from django.contrib import auth
from django.http import JsonResponse, HttpResponseRedirect, HttpResponse
from DjangoCaptcha import Captcha


def login(request):
    """
    登录
    :param request:
    :return:
    """
    if request.method == "GET":
        return render(request, "account/login.html")

    result = {"status": 0, "message": ""}
    try:
        param = json.loads(request.body)
        user = auth.authenticate(**param)
        if not user:
            result["message"] = "用户名或密码错误"
            return JsonResponse(result)

        auth.login(request, user)
        result["status"] = 1
    except:
        result["message"] = "用户名或密码错误"

    return JsonResponse(result)


def logout(request):
    """
    退出登录
    :param request:
    :return:
    """
    auth.logout(request)
    return HttpResponseRedirect("/account/login/")


def code(request):
    """
    产生验证码
    :param request:
    :return:
    """
    def gene_text():
        source = 'ABCDEFGHJKMNPRSTUVWXYZabcdefghjkmnpqrstuvwxyz23456789'
        return ''.join(random.sample(list(source), 4))

    ca = Captcha(request)
    ca.words = [gene_text()]
    ca.type = "word"

    return ca.display()


def set_menu_id(request):
    """
    切换一级菜单
    :param request:
    :return:
    """
    menu_id = int(request.GET.get("menu_id", 0))
    if menu_id:
        request.session["menu_id"] = menu_id
    return HttpResponse("ok")



