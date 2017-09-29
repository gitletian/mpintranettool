# coding: utf-8
from __future__ import unicode_literals
import sys
reload(sys)
sys.setdefaultencoding("utf-8")
import pdb
import MySQLdb
import psycopg2
import os
import pandas as pd
current_dir = os.path.dirname(os.path.abspath(__file__))


"""
获取数据库链接
:param user_id:
:return:
"""

class AttrDescGen:
    def __init__(self):
        self.conn = psycopg2.connect(database="mp_portal",
                                     user="elengjing",
                                     password="Marcpoint2016",
                                     host="192.168.110.11",
                                     port="5432")
        self.cursor = self.conn.cursor()

    def close(self):
        self.cursor.close()
        self.conn.close()

    def count_tag(self):
        sql = '''
        select
        category_id,attr_name,attr_value
        from
        attr_value
        where category_id in
        (
        select id from category
         where
         industry_id = 16
        )
        ;
        '''

        self.cursor.execute(sql)
        c_tag_count = {}
        a_tag_count = {}
        c_a_tag_count = []
        for category_id, attr_name, attr_value in self.cursor.fetchall():
            attr_value_list = attr_value.split(",")
            c_a_tag_count.append(dict(category_id=category_id, attr_name=attr_name, counter=len(attr_value_list)))
            for av in attr_value_list:
                if not c_tag_count.has_key(category_id):
                    c_tag_count[category_id] = set([av])
                else:
                    c_tag_count[category_id].add(av)

                if not a_tag_count.has_key(attr_name):
                    a_tag_count[attr_name] = set([av])
                else:
                    a_tag_count[attr_name].add(av)

        print "========================="
        c_df = pd.DataFrame(columns=("category_id", "counter"))
        a_df = pd.DataFrame(columns=("attr_name", "counter"))
        c_a_df = pd.DataFrame(columns=("category_id", "attr_name", "counter"))
        for k, v in c_tag_count.iteritems():
            c_tag_count[k] = len(v)
            c_df = c_df.append({"category_id": k, "counter": len(v)}, ignore_index=True)

        for k, v in a_tag_count.iteritems():
            a_tag_count[k] = len(v)
            a_df = a_df.append({"attr_name": k, "counter": len(v)}, ignore_index=True)

        for _ in c_a_tag_count:
            c_a_df = c_a_df.append(_, ignore_index=True)

        c_df = c_df.astype("int")
        c_df.to_csv("c_df.csv", index=False, header=False, sep=str("\t"))

        a_df["counter"] = a_df["counter"].astype("int")
        a_df.to_csv("a_df.csv", index=False, header=False, sep=str("\t"))

        c_a_df["counter"] = c_a_df["counter"].astype("int")
        c_a_df["category_id"] = c_a_df["category_id"].astype("int")
        c_a_df.to_csv("c_a_df.csv", index=False, header=False, sep=str("\t"))

    def count_attr(self):
        sql = '''
                select
                category_id,attr_name,attr_value
                from
                attr_value
                where category_id in
                (
                select id from category
                 where
                 industry_id = 16
                )
                ;
                '''
        print "=============count_attr================"
        self.cursor.execute(sql)
        all_attr = {}
        for category_id, attr_name, attr_value in self.cursor.fetchall():
            attr_value_list = attr_value.split(",")
            # attr_name = attr_name.decode("utf8")
            for av in attr_value_list:
                if all_attr.has_key(av):
                    all_attr[av].add(attr_name)
                else:
                    all_attr[av] = set([attr_name])

        output = {}
        for k, v in all_attr.iteritems():
            if(len(v) > 1):
                output[k] = ",".join(list(v))

        ca_df = pd.DataFrame(pd.Series(output))
        ca_df2 = ca_df.reset_index()
        ca_df2.columns = ["attr_value", "attr_name"]
        ca_df2.to_csv('ca_df.csv', index=False, header=False, sep=str("\t"))
if __name__ == '__main__':
    attrdescgen = AttrDescGen()
    # attrdescgen.count_tag()
    attrdescgen.count_attr()
    attrdescgen.close()


