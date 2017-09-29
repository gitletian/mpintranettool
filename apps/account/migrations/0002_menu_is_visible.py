# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('account', '0001_initial'),
    ]

    operations = [
        migrations.AddField(
            model_name='menu',
            name='is_visible',
            field=models.BooleanField(default=True, help_text='\u662f\u5426\u53ef\u89c1'),
        ),
    ]
