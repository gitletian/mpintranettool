# coding: utf-8
# __author__: ""
from __future__ import unicode_literals
import time
from impala import dbapi

start = time.time()
cursor = dbapi.connect(host='172.16.1.14', port=21050, database='test').cursor()
cursor.execute('select itemid, listprice from item_pf limit 10')

print cursor.fetchall()
print 'search:', time.time() - start