# coding: utf-8
# __author__: ""
from __future__ import unicode_literals
from django.http import HttpResponseRedirect, HttpResponse, JsonResponse
from django.conf import settings
import apps.account.api as account_api
import traceback
from tools.logger import Logger
logger = Logger()
"""
拦截所有get和post请求, 检查是否登录及用户权限
"""


class RequiredMiddleware(object):
    def __init__(self):
        return

    @classmethod
    def process_request(cls, request):
        # 排除以下开头的URL
        for url in settings.EXCLUDE_START_URL:
            if request.path.startswith(url):
                return

        if request.method == "GET":
            if request.user.is_authenticated():
                if request.path != "/" and not account_api.check_menu(request):
                    return HttpResponse(u"权限不足, 无权操作")
            else:
                return HttpResponseRedirect("/account/login/")

        elif request.method == "POST":
            if request.user.is_authenticated():
                if request.path != "/" and not account_api.check_menu(request):
                    return JsonResponse({"status": -2, "message": u"权限不足, 无权操作"})
            else:
                return JsonResponse({"status": -1, "message": u"登录已失效, 请重新登录"})


    @classmethod
    def process_exception(cls, request, excpetion):
        # 捕获未处理的异常并发送邮件通知
        traceback.print_exc()
        logger.error(traceback.format_exc())
        return HttpResponse(str(excpetion))

