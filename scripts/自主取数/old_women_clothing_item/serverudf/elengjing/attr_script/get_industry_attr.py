# coding: utf-8
from __future__ import unicode_literals
import MySQLdb
import psycopg2
import json
import top_api
import os
current_dir = os.path.dirname(os.path.abspath(__file__))

CREATE_ATTR_VALUE_NEW_SQL = '''
DROP TABLE IF EXISTS "public"."attr_value_new";
CREATE TABLE "public"."attr_value_new" (
"id" serial,
"industry_id" int8,
"category_id" int4,
"attr_id" int4,
"attr_name" varchar(100) COLLATE "default",
"is_multi" char(10) COLLATE "default",
"attr_value" text COLLATE "default",
"flag" char(5) DEFAULT 'A'::bpchar COLLATE "default",
"is_tag" char(5) DEFAULT 'y'::bpchar COLLATE "default"
)
WITH (OIDS=FALSE);
ALTER TABLE "public"."attr_value_new" OWNER TO "elengjing";

DROP TABLE IF EXISTS "public"."attr_value_bak";
'''

SWAP_ATTR_VALUE_SQL = '''
alter table attr_value rename to attr_value_bak;
alter table attr_value_new rename to attr_value;
'''


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
    return conn


class AttrDescGen:
    def __init__(self):
        """
        获取top api,根据需求指定method 和 fields
        如:需要在测试环境,指定gateway
        :return:
        """
        param = dict(
            method="taobao.itemprops.get",
            fields="pid,name,must,multi,prop_values",
        )
        self.top_api = top_api.Api(secret="374a7a9a5b328df3451b46975a0ecfcb", **param)

    def get_industryattr_json_file(self):
        """
        从数据库导出 attr 到 json文件中
        :return:
        """
        con = get_connect()
        cursor = con.cursor()
        cursor.execute("select industry_id,category_id,attr_name,attr_value from attr_value where industry_id = 16 order by category_id desc")
        attr_value_map = {}
        for IndustryID, CID, AttrName, attr_value in cursor.fetchall():
            if attr_value_map.has_key(str(IndustryID)):
                industry_map = attr_value_map[str(IndustryID)]
                if industry_map.has_key(str(CID)):
                    cid_map = industry_map[str(CID)]
                    if not cid_map.has_key(AttrName):
                        attr_map = {AttrName: attr_value}
                        cid_map.update(attr_map)
                else:
                    cid_map = {str(CID): {AttrName: attr_value}}
                    industry_map.update(cid_map)

            else:
                attr_value = {str(IndustryID): {str(CID): {AttrName: attr_value}}}
                attr_value_map.update(attr_value)
        cursor.close()
        with open(current_dir + "/../industryattr.json", 'w') as f:
            f.write(json.dumps(attr_value_map, ensure_ascii=False, indent=2))

    def get_cids(self, cursor):
        """
        获取所有的categoryid
        :param cursor:
        :return:
        """
        # cursor.execute("select categoryID from category where IndustryID = 16")
        cursor.execute("select id from category where industry_id = 16")
        return [cid[0] for cid in cursor.fetchall()]

    def insert_to_db(self, row, cursor):
        """
        将字典形数据插入到数据库
        :param row:数据 cursor:数据库游标
        :return:
        """
        keys = row.keys()
        sql = "INSERT INTO attr_value_new ({0}) VALUES ({1})".format(",".join(keys), ",".join(["%s" for _ in range(len(keys))]))
        cursor.execute(sql, row.values())

    def parse_resp(self, resp, cid):
        """
        解析request 请求的返回的内容
        :param resp:response ; cid:categoryid
        :return: row_list 需要插入数据库的数据
        """
        row_list = []
        if resp:
            item_prop = resp["item_prop"]
            for prop in item_prop:
                attr_list = []
                if prop.has_key("prop_values"):
                    prop_values = prop.get("prop_values")
                    if prop_values.has_key("prop_value"):
                        prop_value = prop_values.get("prop_value")
                        for attr in prop_value:
                            attr_list.append(attr["name"])
                attr_item = dict(
                    industry_id=16,
                    category_id=cid,
                    attr_id=prop["pid"],
                    attr_name=prop["name"],
                    attr_value=",".join(attr_list),
                    is_multi=prop["multi"],
                )
                row_list.append(attr_item)

        return row_list

    def import_db(self):
        """
        更新数据库的 attr 属性
        :return: row_list 需要插入数据库的数据
        """
        connect = get_connect()
        cursor = connect.cursor()
        cids = self.get_cids(cursor)
        for cid in cids:
            self.top_api.default_params["cid"] = cid
            resp = self.top_api.execute()
            row_list = self.parse_resp(resp, cid)
            for row in row_list:
                self.insert_to_db(row, cursor)

        connect.commit()
        cursor.close()
        connect.close()

    def run(self):
        """
        脚本主方法
        :return:
        """
        connect = get_connect()
        cursor = connect.cursor()

        cursor.execute(CREATE_ATTR_VALUE_NEW_SQL)
        connect.commit()

        self.import_db()

        cursor.execute(SWAP_ATTR_VALUE_SQL)

        connect.commit()
        cursor.close()
        connect.close()

        self.get_industryattr_json_file()

if __name__ == '__main__':
    attrdescgen = AttrDescGen()
    attrdescgen.run()


'''
1、上传industryattr.json 至 192.168.110.122 的目录 /home/script/normal_servers/serverudf/elengjing 下
2、删除hdfs上旧的industryattr.json
    sudo -u hive hdfs dfs -rm -r /data/industryattr.json
3、上传新的industryattr.json 至 /data/ 目录
    sudo -u hive hdfs dfs -put industryattr.json /data/
4、运行打标签脚本,进行打标签
       beeline -u jdbc:hive2://mphd02:10000 -n hive -p hive1 -f /home/script/normal_servers/women_clothing/
'''

