# coding: utf-8
# __author__ = ''
from __future__ import unicode_literals

from django.conf import settings
from tools.common_class import ADict
import pyhs2


class Hive:
    def __init__(self, pool_name='poolHigh'):
        # 设置超时时间30秒无响应即关闭连接
        self.conn = pyhs2.connect(**settings.HIVE_CONNECTION)
        self.pool_name = pool_name

    def get(self, sql):
        rows = self.query(sql.encode('utf8'))
        return rows[0] if len(rows) > 0 else None

    def query(self, sql, meta=False):
        """
        :param sql:
        :param meta: True的时候同时返回表头信息
        :return:
        """
        try:
            with self.conn.cursor() as cursor:
                # 设置pool
                cursor.execute('set mapred.fairscheduler.pool={0}'.format(self.pool_name))
                cursor.execute(sql.encode('utf8'))
                columns = [_['columnName'] for _ in cursor.getSchema()]
                rows = [ADict(zip(columns, _)) for _ in cursor]
                if meta:
                    return rows, columns
                else:
                    return rows
        except Exception, e:
            raise e
        finally:
            self.close()

    def total(self, sql):
        try:
            with self.conn.cursor() as cursor:
                # 设置pool
                cursor.execute('set mapred.fairscheduler.pool={0}'.format(self.pool_name))
                cursor.execute(sql.encode('utf8'))
                columns = [_['columnName'] for _ in cursor.getSchema()]
                rows = [ADict(zip(columns, _)) for _ in cursor]

                if rows:
                    return rows[0][columns[0]]
                else:
                    return 0
        except Exception, e:
            raise e
        finally:
            self.close()

    def close(self):
        self.conn.close()

