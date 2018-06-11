# coding: utf-8
# __author__: ""
from __future__ import print_function, unicode_literals, division
import pandas as pd
import numpy as np
import os
from StringIO import StringIO
from datetime import datetime
from functools import partial
import pdb
import operator

from pandas.core.series import Series


class NewSeries(Series):
    def __init__(self, *kwargs):
        super(NewSeries, self).__init__(*kwargs)

    def __and__(self, othen):
        s = map(operator.and_(), self, othen)
        return NewSeries(s)


def average(df):
    pass


def avg_weighted(column, factor):
    '''
    利用 pandas 计算 加权平均
    :param column:
    :param cover_user_scale:
    :return:
    '''
    column_mul = column * factor
    return column_mul.sum() / factor.sum()

if __name__ == "__main__":

    df = pd.DataFrame([
        {"a": 2.3232, "b": 2, "c": 2, "d": False},
        {"a": 2.323, "b": 5, "c": 1, "d": True},
        {"a": None, "b": 2, "c": 1, "d": False},
        {"a": 4, "b": 5, "c": 3, "d": True},
    ])

    df2 = pd.DataFrame([
        {"a": 2.3232, "b": 2},
        {"a": 2.323, "b": 5},
        {"a": None, "b": 2},
        {"a": 4, "b": 5},
    ])

    df3 = pd.DataFrame([
        {"a": 2.3232, "b": 2},
        {"a": 2.323, "b": 5},
        {"a": None, "b": 2},
        {"a": 4, "b": 5},
    ])

    df = pd.DataFrame([
        {"a": 0, "b": "432", "c": "122", "d": False, "e": 0},
        {"a": 1, "b": "32423", "c": "0", "d": True, "e": 0},
        {"a": 0, "b": "3242", "c": "323", "d": False, "e": 0},
        {"a": 1, "b": "342", "c": "32", "d": True, "e": 0},
        {"a": 1, "b": "342", "c": "32", "d": True, "e": 1},
        {"a": 0, "b": "342", "c": "32", "d": True, "e": 0},
        {"a": 0, "b": "342", "c": "32", "d": True, "e": 0},
    ])

    df.groupby()

    arr = np.arange(32).reshape((8, 4))
    arr.swapaxes()


    # df[["a", "b"]].apply(lambda x: x * df["c"], axis=0)

    # df[["a", "b"]].apply(lambda x: x["a"] if x["d"] else x["b"], axis=1)

    # for _ in ["a", "c"]:
    #     df['{}b'.format(_)] = df[_] * df['b']
    #
    # print(df)

    # print(df[["a", "b"]].apply(partial(avg_weighted, df.c), axis=0))

    # print(df[["a", "b"]].apply(avg_weighted, axis=0, args=(3)))

    # print df.columns


df_data = df.loc[:, ["a", "b", "c"]]
df_data2= df.loc[:, ["a", "b", "c"]]
df_sum = df.loc[:, ["a", "b", "c"]].sum(axis=1)



df_data2["all"] = df_data2.sum(axis=1)


def f(row):
    return [_ / row[3] for _ in row]


df_data.apply(lambda x: x / x.sum(), axis=1)



