# coding: utf-8
# __author__: ""
from __future__ import unicode_literals
from django.conf import settings
import apps.account.api as account_api


"""
在这里设置一些供模板访问的全局变量
"""


def variable(request):
    content = {
        "path": request.get_full_path(),
        "version": settings.STATIC_FILE_VERSION,
        "site_name": settings.SITE_NAME
    }
    if request.user.is_authenticated():
        content.update({
            "menus": account_api.get_menus(request),
            "menuid": account_api.get_menu_id(request),
            "menuname": account_api.get_menu_name(request),
        })
    return content

