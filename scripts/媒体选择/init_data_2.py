# coding: utf-8
from __future__ import unicode_literals
import sys
reload(sys)
sys.setdefaultencoding("utf-8")
import psycopg2
import os
import pdb
import pandas as pd
from datetime import datetime
import random


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
    # conn = psycopg2.connect(database="mp_portal", user="elengjing", password="Marcpoint2016", host="192.168.110.11", port="5432")

    conn = psycopg2.connect(database="media_choice", user="media_choice", password="Marcpoint2016", host="192.168.110.12",
                            port="5432")
    # conn.set_client_encoding("UTF8")
    return conn


class InitData:
    '''
    初始化媒体选择数据
    '''

    def platform_base(self, platformids):
        """
        初始化 platform_base_index
        :return:
        """

        sql = '''
                    insert into
                    platform_base_index(platform_id, pregnancy_code, area_code, pass_month, cover_user_scale, active_users, social_force, nosie_scale, loast_update_time)
                    VALUES({0}, {1}, {2}, {3}, {4}, {5}, {6}, {7}, '{8}')
                    '''

        con = get_connect()
        try:
            cursor = con.cursor()
            for i in range(1, 7):
                for n in range(1, 9):
                    for j in range(1, 13):
                        for platformid in platformids:
                            date = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                            cursor.execute(sql.format(platformid, i, n, j,
                                                      random.randint(1000, 100000),
                                                      random.randint(100, 10000),
                                                      random.randint(50, 5000),
                                                      random.randint(100, 5000),
                                                      date
                                                      ))

            con.commit()

        except Exception:
            raise
        finally:
            con.close()

    def platform_category(self, platformids):
        """
        初始化 platform_category_index
        :return:
        """

        sql = '''
                    insert into
                    platform_category_index(platform_id, pregnancy_code, area_code, pass_month, category_id, attention_category, loast_update_time)
                    VALUES({0}, {1}, {2}, {3}, {4}, {5}, '{6}')
                    '''

        con = get_connect()
        try:
            cursor = con.cursor()
            for i in range(1, 7):
                for n in range(1, 9):
                    for j in range(1, 13):
                        for k in range(1, 3):
                            for platformid in platformids:
                                date = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                                cursor.execute(sql.format(platformid, i, n, j, k,
                                                          random.randint(10, 1000),
                                                          date
                                                          ))

            con.commit()

        except Exception:
            raise
        finally:
            con.close()

    def platform_brand(self, platformids):
        """
        初始化 platform_brand_index
        :return:
        """

        sql = '''
                     insert into
                     platform_brand_index(platform_id, pregnancy_code, area_code, pass_month, category_id, brand_code, awareness, fancy, sentiment, loast_update_time)
                     VALUES({0}, {1}, {2}, {3}, {4}, {5}, {6}, {7}, {8}, '{9}')
                     '''

        con = get_connect()
        try:
            cursor = con.cursor()
            for i in range(1, 7):
                for n in range(1, 9):
                    for j in range(1, 13):
                        for k in range(1, 3):
                            for v in range(1, 5):
                                for platformid in platformids:
                                    date = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                                    cursor.execute(sql.format(platformid, i, n, j, k, v,
                                                              random.randint(1, 100),
                                                              random.randint(1, 100),
                                                              random.randint(10, 1000),
                                                              date
                                                              ))

            con.commit()

        except Exception:
            raise
        finally:
            con.close()

    def subject_base(self, platformids):
        """
        初始化 subject_base_index
        :return:
        """

        sql = '''
                    insert into
                    subject_base_index(platform_id, pregnancy_code, area_code, pass_month, subject, cover_user_scale, active_users, kol, grass_kol, loast_update_time)
                    VALUES({0}, {1}, {2}, {3}, '{4}', {5}, {6}, {7}, {8}, '{9}')
                    '''

        con = get_connect()
        try:
            cursor = con.cursor()
            for i in range(1, 7):
                for n in range(1, 9):
                    for j in range(1, 13):
                        for platformid in platformids:
                            for subject in ["首页", "孕晚期妈咪", "孕中期妈咪", "待产包攻略"]:
                                date = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                                cursor.execute(sql.format(platformid, i, n, j, subject,
                                                          random.randint(50, 5000),
                                                          random.randint(5, 500),
                                                          random.randint(1, 100),
                                                          random.randint(1, 100),
                                                          date
                                                          ))

            con.commit()

        except Exception:
            raise
        finally:
            con.close()

    def subject_category(self, platformids):
        """
        初始化 subject_category_index
        :return:
        """

        sql = '''
                    insert into
                    subject_category_index(platform_id, pregnancy_code, area_code, pass_month, category_id, subject, attention_category, loast_update_time)
                    VALUES({0}, {1}, {2}, {3}, {4}, '{5}', {6}, '{7}')
                    '''

        con = get_connect()
        try:
            cursor = con.cursor()
            for i in range(1, 7):
                for n in range(1, 9):
                    for j in range(1, 13):
                        for k in range(1, 3):
                            for platformid in platformids:
                                for subject in ["首页", "孕晚期妈咪", "孕中期妈咪", "待产包攻略"]:
                                    date = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                                    cursor.execute(sql.format(platformid, i, n, j, k, subject,
                                                              random.randint(5, 500),
                                                              date
                                                              ))

            con.commit()

        except Exception:
            raise
        finally:
            con.close()

    def subject_brand(self, platformids):
        """
        初始化 subject_brand_index
        :return:
        """

        sql = '''
                     insert into
                     subject_brand_index(platform_id, pregnancy_code, area_code, pass_month, category_id, brand_code, subject, awareness, fancy, sentiment, loast_update_time)
                     VALUES({0}, {1}, {2}, {3}, {4}, {5}, '{6}', {7}, {8}, {9}, '{10}')
                     '''

        con = get_connect()
        try:
            cursor = con.cursor()
            for i in range(1, 7):
                for n in range(1, 9):
                    for j in range(1, 13):
                        for k in range(1, 3):
                            for v in range(1, 5):
                                for platformid in platformids:
                                    for subject in ["首页", "孕晚期妈咪", "孕中期妈咪", "待产包攻略"]:
                                        date = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                                        cursor.execute(sql.format(platformid, i, n, j, k, v, subject,
                                                                  random.randint(1, 50),
                                                                  random.randint(1, 50),
                                                                  random.randint(30, 100),
                                                                  date
                                                                  ))

            con.commit()

        except Exception:
            raise
        finally:
            con.close()

    def get_platformids(self):
        con = get_connect()
        cursor = con.cursor()
        cursor.execute("select id from platform where industry_id = 1")
        platformids = [id[0] for id in cursor.fetchall()]
        con.close()
        return platformids

if __name__ == '__main__':
    initData = InitData()
    platformids = initData.get_platformids()
    initData.platform_base(platformids)
