# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
    ]

    operations = [
        migrations.CreateModel(
            name='Menu',
            fields=[
                ('id', models.AutoField(verbose_name='ID', serialize=False, auto_created=True, primary_key=True)),
                ('name', models.CharField(help_text='\u83dc\u5355\u540d\u79f0', max_length=256)),
                ('pid', models.IntegerField(help_text='\u7236\u83dc\u5355ID', null=True)),
                ('url', models.CharField(help_text='\u83dc\u5355\u5bf9\u5e94\u7684url', max_length=256, null=True, blank=True)),
                ('seq', models.IntegerField(help_text='\u83dc\u5355\u7684\u6392\u5e8f\u5e8f\u53f7')),
                ('is_enabled', models.BooleanField(default=True, help_text='\u662f\u5426\u7981\u7528')),
            ],
            options={
                'db_table': 'auth_menu',
            },
        ),
    ]
