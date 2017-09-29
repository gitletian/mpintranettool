# coding: utf-8
# __author__: ""
from __future__ import unicode_literals
from django.db import models

# Create your models here.


class Menu(models.Model):
    name = models.CharField(help_text="菜单名称", max_length=256)
    pid = models.IntegerField(help_text="父菜单ID", null=True)
    url = models.CharField(help_text="菜单对应的url", max_length=256, null=True, blank=True)
    seq = models.IntegerField(help_text="菜单的排序序号")
    is_enabled = models.BooleanField(help_text="是否禁用", default=True)
    is_visible = models.BooleanField(help_text="是否可见", default=True)

    class Meta:
        db_table = "auth_menu"


