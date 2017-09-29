# coding: utf-8
# __author__: ""
from __future__ import unicode_literals

from django.conf.urls import url
import views


urlpatterns = [
    url(r"^login/$", views.login),
    url(r"^logout/$", views.logout),
    url(r"^set-menu-id/$", views.set_menu_id),
    url(r"^code/$", views.code),

]

