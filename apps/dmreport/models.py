# coding: utf-8
# __author__: ""
from __future__ import unicode_literals
from django.db import models


class Industry(models.Model):
    id = models.BigIntegerField(help_text="行业ID", primary_key=True)
    name = models.CharField(help_text="行业名", max_length=256)
    db_name = models.CharField(help_text="数据库中使用的表前缀", max_length=256)
    create_time = models.DateTimeField(help_text="创建时间", blank=True, null=True)
    update_time = models.DateTimeField(help_text="更新时间", blank=True, null=True)
    query_type = models.CharField(help_text="y=按年, m=按月, d=按天", max_length=256)
    start_date = models.DateField(help_text="hive表min(daterange)")
    end_date = models.DateField(help_text="hive表max(daterange)")

    class Meta:
        db_table = "industry"


class Category(models.Model):
    id = models.BigIntegerField(help_text="品类ID", primary_key=True)
    pid = models.BigIntegerField(help_text="品类父节点", blank=True, null=True)
    name = models.CharField(help_text="品类名", max_length=256)
    industry = models.ForeignKey(Industry, help_text="行业ID", on_delete=models.DO_NOTHING)
    grade = models.IntegerField(help_text="类目", blank=True, null=True)
    is_leaf = models.NullBooleanField(help_text="是否叶节点", )
    leaf_ids = models.CharField(help_text="所有叶子节点, 不包含自己", max_length=512, null=True)
    sub_category_ids = models.CharField(help_text="叶子节点的子品类ID", max_length=4000, blank=True, null=True)
    create_time = models.DateTimeField(help_text="创建时间", blank=True, null=True)
    update_time = models.DateTimeField(help_text="更新时间", blank=True, null=True)

    class Meta:
        db_table = "category"


class Shop(models.Model):
    id = models.IntegerField(help_text="店铺ID", primary_key=True)
    name = models.CharField(help_text="店铺名", max_length=256)
    brand = models.CharField(help_text="品牌", max_length=256)
    platform = models.CharField(help_text="平台(数据源)", blank=True, null=True, max_length=256)
    url = models.CharField(help_text="店铺地址", max_length=256, blank=True, null=True)
    is_registered = models.BooleanField(help_text="店铺是否已被用户认证", default=False)
    create_time = models.DateTimeField(help_text="创建时间", blank=True, null=True, auto_now_add=True)
    update_time = models.DateTimeField(help_text="更新时间", blank=True, null=True, auto_now_add=True)

    class Meta:
        db_table = "shop"
