# coding: utf-8
from __future__ import unicode_literals
import sys
import dbconnect as db
from sql import *
reload(sys)
sys.setdefaultencoding("utf-8")


category_id = 50000697
'''
执行 attrname 脚本,提取数据
'''
db.hive_query(attrname_sql.format(category_id), is_select=False)
rows = db.hive_query(get_attrname_sql)
if len(rows) > 0:
    attrnames = rows[0]["attrname"]
    rows = db.pg_query(get_text_vlaue_sql.format('attrname_columns'))
    if len(rows) > 0:
        save_text_vlaue = update_text_vlaue_sql.format(attrnames, 'attrname_columns')
    else:
        save_text_vlaue = insert_text_vlaue_sql.format('attrname_columns', attrnames)

    db.pg_query(save_text_vlaue, iscommit=True)


'''
执行 attrvalue 脚本,提取数据
'''
db.hive_query(attrvalue_sql.format(category_id), is_select=False)
rows = db.hive_query(get_attrvalue_sql)
if len(rows) > 0:
    attrvalues = rows[0]["attrvalue"]

    rows = db.pg_query(get_text_vlaue_sql.format('attrname_attrvalue_columns'))
    if len(rows) > 0:
        save_text_vlaue = update_text_vlaue_sql.format(attrvalues, 'attrname_attrvalue_columns')
    else:
        save_text_vlaue = insert_text_vlaue_sql.format('attrname_attrvalue_columns', attrvalues)
    db.pg_query(save_text_vlaue, iscommit=True)

