# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('dmreport', '0001_initial'),
    ]

    operations = [
        migrations.CreateModel(
            name='Shop',
            fields=[
                ('id', models.IntegerField(help_text='\u5e97\u94faID', serialize=False, primary_key=True)),
                ('name', models.CharField(help_text='\u5e97\u94fa\u540d', max_length=256)),
                ('brand', models.CharField(help_text='\u54c1\u724c', max_length=256)),
                ('platform', models.CharField(help_text='\u5e73\u53f0(\u6570\u636e\u6e90)', max_length=256, null=True, blank=True)),
                ('url', models.CharField(help_text='\u5e97\u94fa\u5730\u5740', max_length=256, null=True, blank=True)),
                ('is_registered', models.BooleanField(default=False, help_text='\u5e97\u94fa\u662f\u5426\u5df2\u88ab\u7528\u6237\u8ba4\u8bc1')),
                ('create_time', models.DateTimeField(help_text='\u521b\u5efa\u65f6\u95f4', auto_now_add=True, null=True)),
                ('update_time', models.DateTimeField(help_text='\u66f4\u65b0\u65f6\u95f4', auto_now_add=True, null=True)),
            ],
            options={
                'db_table': 'shop',
            },
        ),
    ]
