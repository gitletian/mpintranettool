# coding: utf-8
from __future__ import unicode_literals
import sys
reload(sys)
sys.setdefaultencoding("utf-8")
import psycopg2
import pyhs2


"""
获取数据库链接
:param user_id:
:return:
"""


def pg_query(sqls, iscommit=False, meta=False):
    '''
     执行pg 或mysql  的sql

    :param sqls:
    :param ifcommit:
    :param meta:
    :return:
    '''
    # conn = MySQLdb.connect(host="localhost", user="root", passwd="root", db="mp_portal", charset="utf8")
    # conn = MySQLdb.connect(host="172.16.1.120", user="dev", passwd="Dev_123123", db="mp_portal", charset="utf8")
    # conn = psycopg2.connect(database="mp_portal", user="elengjing", password="Marcpoint2016", host="192.168.110.12", port="5432")
    conn = psycopg2.connect(database="mp_portal", user="elengjing", password="Marcpoint2016", host="192.168.110.11", port="5432")
    try:
        with conn.cursor() as cursor:
            for sql in sqls.split(";"):
                if sql.strip():
                    cursor.execute(sql.encode('utf8'))

            if iscommit:
                conn.commit()
            else:
                columns = [_[0].lower() for _ in cursor.description]
                rows = [dict(zip(columns, _)) for _ in cursor]
                if meta:
                    return rows, columns
                else:
                    return rows
    except Exception, e:
        raise e
    finally:
        conn.close()


def hive_query(sqls, is_select=True, meta=False):
    '''
    执行 hive 的sql
    :param sqls:
    :param is_select: 是否是select
    :param meta:
    :return:
    '''
    hive_connection = {
        "database": "elengjing",
        "host": "192.168.110.122",
        "user": "hive",
        "password": "hive1",
        "port": 10000,
        "authMechanism": "PLAIN"
    }
    conn = pyhs2.connect(**hive_connection)
    try:
        with conn .cursor() as cursor:
            cursor.execute('set ngmr.partition.automerge=true')
            cursor.execute('set ngmr.partition.mergesize.mb=200')

            for sql in sqls.split(";"):
                if sql.strip():
                    cursor.execute(sql.strip().encode('utf8'))

            if is_select:
                columns = [_['columnName'] for _ in cursor.getSchema()]
                rows = [dict(zip(columns, _)) for _ in cursor]
                if meta:
                    return rows, columns
                else:
                    return rows
    except Exception, e:
        raise e
    finally:
        conn.close()
