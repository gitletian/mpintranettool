# coding: utf-8
# __author__: ""
from __future__ import unicode_literals

from django.conf.urls import url
import views

urlpatterns = [
    url(r"^item/main/$", views.test),
    url(r"^item/diff-main/$", views.item_diff_main),
    url(r"^item/diff-content/$", views.item_diff_content, {'template_name': 'dmreport/item_diff_content.html'}),
    url(r"^shop/diff-main/$", views.shop_diff_main),
    url(r"^shop/diff-content/$", views.shop_diff_content, {'template_name': 'dmreport/shop_diff_content.html'}),
    url(r"^category/diff-main/$", views.category_diff_main),
    url(r"^category/diff-content/$", views.category_diff_content, {'template_name': 'dmreport/category_diff_content.html'}),

]

