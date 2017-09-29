# coding: utf-8
# __author__ = "Rich"
from __future__ import unicode_literals

from django.db import connections


class DB(object):
    def __init__(self, sql, name="default"):
        self.conn = connections[name]
        self.cursor = self.conn.cursor()
        self.cursor.execute(sql)

    def query(self):
        columns = [_[0].lower() for _ in self.cursor.description]
        results = [dict(zip(columns, _)) for _ in self.cursor]
        return results

    def total(self):
        columns = [_[0].lower() for _ in self.cursor.description]
        results = [dict(zip(columns, _)) for _ in self.cursor]
        return results[0][columns[0]]

    def execute(self):
        self.conn.commit()

    def __del__(self):
        self.cursor.close()
        self.conn.close()

