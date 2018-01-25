# coding: utf-8
# __author__: ""

import pandas as pd
import numpy as np
import os
from StringIO import StringIO
from datetime import datetime


def test1():
    '''
    Pandas提供了duplicated、Index.duplicated、drop_duplicates函数来标记及删除重复记录
    duplicated函数用于标记Series中的值、DataFrame中的记录行是否是重复，重复为True，不重复为False
    pandas.DataFrame.duplicated(self, subset=None, keep='first')
    pandas.Series.duplicated(self, keep='first')
    其中参数解释如下：
    subset：用于识别重复的列标签或列标签序列，默认所有列标签
    keep=‘frist’：除了第一次出现外，其余相同的被标记为重复
    keep='last'：除了最后一次出现外，其余相同的被标记为重复
    keep=False：所有相同的都被标记为重复
    :return:
    '''
    # 标记DataFrame重复例子
    df = pd.DataFrame(
        {'col1': ['one', 'one', 'two', 'two', 'two', 'three', 'four']
            , 'col2': [1, 2, 1, 2, 1, 1, 1]
            , 'col3': ['AA', 'BB', 'CC', 'DD', 'EE', 'FF', 'GG']
            , 'col4': ['2017-01-01', '2017-01-02', '2017-01-03', '2017-01-05', '2017-01-06', '2017-01-08', '2017-01-10']
         }
        , index=['a', 'a', 'b', 'c', 'b', 'a', 'c'])
    # duplicated(self, subset=None, keep='first')
    # 根据列名标记
    # keep='first'
    df.duplicated()  # 默认所有列，无重复记录
    df.duplicated('col1')  # 第二、四、五行被标记为重复
    df.duplicated(['col1', 'col2'])  # 第五行被标记为重复
    # keep='last'
    df.duplicated('col1', 'last')  # 第一、三、四行被标记重复
    df.duplicated(['col1', 'col2'], keep='last')  # 第三行被标记为重复
    # keep=False
    df.duplicated('col1', False)  # Series([True,True,True,True,True,False,False],index=['a','a','b','c','b','a','c'])
    df.duplicated(['col1', 'col2'], keep=False)  # 在col1和col2列上出现相同的，都被标记为重复
    type(df.duplicated(['col1', 'col2'], keep=False))  # pandas.core.series.Series
    # 根据索引标记
    df.index.duplicated()  # 默认keep='first',第二、五、七行被标记为重复
    df.index.duplicated(keep='last')  # 第一、二、三、四被标记为重复
    df[df.index.duplicated()]  # 获取重复记录行
    df[~df.index.duplicated('last')]  # 获取不重复记录行
    # 标记Series重复例子
    # duplicated(self, keep='first')
    s = pd.Series(['one', 'one', 'two', 'two', 'two', 'three', 'four'], index=['a', 'a', 'b', 'c', 'b', 'a', 'c'],
                  name='sname')
    s.duplicated()
    s.duplicated('last')
    s.duplicated(False)
    # 根据索引标记
    s.index.duplicated()
    s.index.duplicated('last')
    s.index.duplicated(False)


    '''
    drop_duplicates函数用于删除Series、DataFrame中重复记录，并返回删除重复后的结果
    pandas.DataFrame.drop_duplicates(self, subset=None, keep='first', inplace=False)
    pandas.Series.drop_duplicates(self, keep='first', inplace=False)
    '''
    # 删除DataFrame重复记录例子
    # drop_duplicates(self, subset=None, keep='first', inplace=False)
    df.drop_duplicates()
    df.drop_duplicates('col1')  # 删除了df.duplicated('col1')标记的重复记录
    df.drop_duplicates('col1', 'last')  # 删除了df.duplicated('col1','last')标记的重复记录
    df.drop_duplicates(['col1', 'col2'])  # 删除了df.duplicated(['col1','col2'])标记的重复记录
    df.drop_duplicates('col1', keep='last', inplace=True)  # inplace=True表示在原DataFrame上执行删除操作
    df.drop_duplicates('col1', keep='last', inplace=False)  # inplace=False返回一个副本
    # 删除Series重复记录例子
    # drop_duplicates(self, keep='first', inplace=False)
    s.drop_duplicates()

    df.sort()