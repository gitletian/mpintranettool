# coding: utf-8
# __author__: ""
from __future__ import unicode_literals

from django.conf.urls import include, url
from django.contrib import admin

urlpatterns = [
    url(r'^$', 'apps.common.views.static_html', {'template_name': 'index.html'}),
    url(r'^admin/', include(admin.site.urls)),
    url(r'^test/', include('apps.common.urls')),
    url(r'^account/', include('apps.account.urls')),
    url(r'^dmreport/', include('apps.dmreport.urls')),

]


