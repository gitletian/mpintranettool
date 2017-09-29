# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    dependencies = [
    ]

    operations = [
        migrations.CreateModel(
            name='Category',
            fields=[
                ('id', models.BigIntegerField(help_text='\u54c1\u7c7bID', serialize=False, primary_key=True)),
                ('pid', models.BigIntegerField(help_text='\u54c1\u7c7b\u7236\u8282\u70b9', null=True, blank=True)),
                ('name', models.CharField(help_text='\u54c1\u7c7b\u540d', max_length=256)),
                ('grade', models.IntegerField(help_text='\u7c7b\u76ee', null=True, blank=True)),
                ('is_leaf', models.NullBooleanField(help_text='\u662f\u5426\u53f6\u8282\u70b9')),
                ('leaf_ids', models.CharField(help_text='\u6240\u6709\u53f6\u5b50\u8282\u70b9, \u4e0d\u5305\u542b\u81ea\u5df1', max_length=512, null=True)),
                ('sub_category_ids', models.CharField(help_text='\u53f6\u5b50\u8282\u70b9\u7684\u5b50\u54c1\u7c7bID', max_length=4000, null=True, blank=True)),
                ('create_time', models.DateTimeField(help_text='\u521b\u5efa\u65f6\u95f4', null=True, blank=True)),
                ('update_time', models.DateTimeField(help_text='\u66f4\u65b0\u65f6\u95f4', null=True, blank=True)),
            ],
            options={
                'db_table': 'category',
            },
        ),
        migrations.CreateModel(
            name='Industry',
            fields=[
                ('id', models.BigIntegerField(help_text='\u884c\u4e1aID', serialize=False, primary_key=True)),
                ('name', models.CharField(help_text='\u884c\u4e1a\u540d', max_length=256)),
                ('db_name', models.CharField(help_text='\u6570\u636e\u5e93\u4e2d\u4f7f\u7528\u7684\u8868\u524d\u7f00', max_length=256)),
                ('create_time', models.DateTimeField(help_text='\u521b\u5efa\u65f6\u95f4', null=True, blank=True)),
                ('update_time', models.DateTimeField(help_text='\u66f4\u65b0\u65f6\u95f4', null=True, blank=True)),
                ('query_type', models.CharField(help_text='y=\u6309\u5e74, m=\u6309\u6708, d=\u6309\u5929', max_length=256)),
                ('start_date', models.DateField(help_text='hive\u8868min(daterange)')),
                ('end_date', models.DateField(help_text='hive\u8868max(daterange)')),
            ],
            options={
                'db_table': 'industry',
            },
        ),
        migrations.AddField(
            model_name='category',
            name='industry',
            field=models.ForeignKey(on_delete=django.db.models.deletion.DO_NOTHING, to='dmreport.Industry', help_text='\u884c\u4e1aID'),
        ),
    ]
