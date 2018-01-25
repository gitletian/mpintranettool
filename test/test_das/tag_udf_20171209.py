# coding: utf-8
# __author__: 'Rich'
from __future__ import unicode_literals
import sys
import os
import re
import json
import pandas as pd
import traceback
import MySQLdb
import datetime
import operator
from pandas.core.series import Series
import pdb
import json

reload(sys)
sys.setdefaultencoding("utf-8")

class ms(Series):
    def __init__(self, *kwargs):
        super(ms, self).__init__(*kwargs)

    def __and__(self, other):
        return ms(map(operator.and_, self, other))

    def __sub__(self, other):
        return ms(map(lambda x, y: 0 if x - y < 0 else x - y, self, other))

class Tagging:
    def __init__(self, rules_name=None):
        #标点符号, 用于切分短句
        self.punctuation = '，|,|。|;|；|!|！|…'
        #运算符
        self.opts = {
            '[&]': '&',
            '[|]': '|',
            '[-]': '-',
            '(': '(',
            ')': ')'
        }

        data = self.search(rules_name)
        self.tags = json.loads(data['tags'])
        self.dimensions = json.loads(data['dimensions'])
        self.objects = json.loads(data['objs'])

    def search(self, rules_name):
        connection_name = {
            'db': 'das',
            'user': 'das',
            'passwd': '123',
            'host': '172.16.1.100',
            'port': 3306,
            'charset': 'utf8'
        }
        dbs = MySQLdb.connect(**connection_name)
        cursor = dbs.cursor()
        cursor.execute("select tags, dimensions, objs from rules where name='{0}'".format(rules_name))
        columns = [_[0].lower() for _ in cursor.description]
        rows = [dict(zip(columns, _)) for _ in cursor]
        cursor.close()

        return rows[0] if len(rows) > 0 else None

    def __opsplit(self, exp):
        for k, v in self.opts.iteritems():
            exp = exp.replace(k, '\t{0}\t'.format(v))
        return [_ for _ in exp.split('\t') if _]

    def __match(self, pattern, x):
        try:
            return 1 if re.search(pattern, x, flags=re.I) else 0
        except:
            return 0

    def read_content(self, path):
        # self.df = pd.DataFrame(lines, columns=['id', 'content', 'category_id'])
        self.df = pd.read_csv(path, sep='\t', names=['id', 'content', 'category_id'], header=None)

    #按叶子词库打标签
    def tagging_ck(self):
        print '词库: ' + json.dumps(self.tags, encoding='utf-8', ensure_ascii=False)
        for tag, keywords in self.tags.iteritems():
            self.df[tag] = self.df.content.apply(lambda x: self.__match(keywords, x))

        self.df["is_taged"] = self.df.loc[:, self.objects]
        self.df.to_excel('ck1.xlsx', index=False)

    #切分短句并重打标签
    #汇总当前对象打上标签的标签个数
    #如果任何一个对象的个数超过1个则需要切分
    def tagging_cut(self):
        if len(tg.objects) == 0:
            return

        for object_name in self.objects:
            columns = [_ for _ in self.df.columns if object_name in _]
            self.df[object_name] = self.df.loc[:, columns].sum(axis=1).apply(lambda x: x > 1)

        self.df['need_cut'] = self.df.loc[:, self.objects].any(axis=1)

        df_new = self.df[self.df.need_cut==True]

        if len(df_new) == 0:
            return

        rows = []
        for _, row in df_new.iterrows():
            for text in re.split(self.punctuation, row['content'], flags=re.I):
                if text:
                    rows.append({'id': row.id, 'content': text, 'category_id': row.category_id})

        df_cuted = pd.DataFrame(rows)

        #切分的行重打标签
        for tag, keywords in self.tags.iteritems():
            df_cuted[tag] = df_cuted.content.apply(lambda x: self.__match(keywords, x))

        df_nocut = self.df[self.df.need_cut==False]
        self.df = pd.concat([df_nocut, df_cuted])


    #按维度打标签
    def tagging_by_wd(self):
        print '维度: ' + json.dumps(self.dimensions, encoding='utf-8', ensure_ascii=False)
        for name, rule in self.dimensions.iteritems():
            if name in self.tags.keys():
                continue

            opt_lst = [_ for _ in self.__opsplit(rule)]
            for i in range(len(opt_lst)):
                if opt_lst[i] in self.opts.values():
                    continue

                #如果带*号则需要处理
                if '*' in opt_lst[i]:
                    columns = [_ for _ in self.tags.keys() if _.startswith(opt_lst[i].replace('*', ''))]
                    opt_lst[i] = 'ms(self.df.loc[:, ["{0}"]].any(axis=1).apply(lambda x: int(x)))'.format('","'.join(columns))
                else:
                    opt_lst[i] = 'ms(self.df["{0}"])'.format(opt_lst[i])

            self.df[name] = eval(''.join(opt_lst))

    def print_result(self):
        #如果有对象则先把切短过短句的合并
        if len(self.objects) > 0:
            self.df = self.df.groupby(['category_id', 'id'], as_index=False).sum()


        clumns = self.df.columns.tolist()
        clumns.remove('category_id')
        self.df['has_data'] = self.df.loc[:, clumns].any(axis=1)
        self.df.to_excel('ck2.xlsx', index=False)

        for _, row in self.df.iterrows():
            for tag in self.dimensions.keys():
                if row[tag] > 0:
                    print '\t'.join([unicode(row['id']), tag, row["category_id"]])


if __name__ == '__main__':
    tg = Tagging('media_brand_action_baby_food.xlsx')
    path = '/Users/guoyuanpei/Documents/test_cut6.csv'

    print('读取数据')
    tg.read_content(path)

    print('按词库打标签')
    tg.tagging_ck()

    print('切分短句')
    tg.tagging_cut()

    print('按维度打标签')
    tg.tagging_by_wd()

    print '输出结果'
    tg.print_result()

