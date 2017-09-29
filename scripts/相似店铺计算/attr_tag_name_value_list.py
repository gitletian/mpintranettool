# coding: utf-8
from __future__ import unicode_literals
import sys
reload(sys)
sys.setdefaultencoding("utf-8")
import psycopg2
import os
import pdb
import pandas as pd
current_dir = os.path.dirname(os.path.abspath(__file__))

"""
获取数据库链接
:param user_id:
:return:
"""
def get_connect():
    # conn = MySQLdb.connect(host="localhost", user="root", passwd="root", db="mp_portal", charset="utf8")
    # conn = MySQLdb.connect(host="172.16.1.120", user="dev", passwd="Dev_123123", db="mp_portal", charset="utf8")
    # conn = psycopg2.connect(database="mp_portal", user="elengjing", password="Marcpoint2016", host="192.168.110.12", port="5432")
    conn = psycopg2.connect(database="mp_portal", user="elengjing", password="Marcpoint2016", host="192.168.110.11", port="5432")
    # conn.set_client_encoding("UTF8")
    return conn


class AttrDescGen:
    def get_industryattr_csv_file(self):
        """
        从数据库导出 attr 到 json文件中
        :return:
        """
        con = get_connect()
        cursor = con.cursor()
        cursor.execute("select attr_name,attr_value from attr_value where industry_id = 16 and category_id = 50008899 and attr_name != '品牌' order by category_id desc")
        attr_set = set()
        for AttrName, attr_value in cursor.fetchall():
            if attr_value:
                [attr_set.add(":".join([AttrName, attr_v])) for attr_v in attr_value.split(",")]
        cursor.close()
        df = pd.DataFrame(list(attr_set), columns=['name'])
        df.to_csv("attr_value.csv", index=False, header=False, sep=str("\t"))

    def run(self):
        """
        脚本主方法
        :return:
        """
        self.get_industryattr_csv_file()


if __name__ == '__main__':
    attrdescgen = AttrDescGen()
    attrdescgen.run()
