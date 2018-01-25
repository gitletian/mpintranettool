# coding: utf-8
# __author__: ""

import pandas as pd
import numpy as np
import os
from StringIO import StringIO
from datetime import datetime


def test1():
    '''
    Dateframe 数据初始化
    1、 Series 是一列多行
        Series是一个一维的类似的数组对象，包含一个数组的数据（任何NumPy的数据类型）和一个与数组关联的数据标签，被叫做 索引 。最简单的Series是由一个数组的数据构成
        另一种思考的方式是，Series是一个定长的，有序的字典，因为它把索引和值映射起来了。它可以适用于许多期望一个字典的函数
        Series 在算术用算中它会自动对齐不同索引的数据

        类似于 Numpy 的核心是 ndarray，pandas 也是围绕着 Series 和 DataFrame. 两个核心数据结构展开的 。Series 和 DataFrame. 分别对应于一维的序列和二维的表结构

    2、 Dateframe 是多列多行

    3、 pandas建造在NumPy之上



    :return:
    '''
    dates = pd.date_range("20160910", periods=6)
    #初始化 1, 使用  二维数组, index, 和 columns
    df = pd.DataFrame(np.random.randn(6, 4), index=dates, columns=list("ABCD"))
    #初始化 2, 使用 字典类型  value 为数组, 但是 长度必须一致
    df2 = pd.DataFrame({"A": np.random.randn(6)})
    #初始化 2, 使用 使用字典类型,至少有一个value 为 Series 类型(即 pd.Series 的结果)
    df2 = pd.DataFrame({"A":pd.Timestamp("20161010"), "B": pd.Series(1)})
    #初始化 2, 使用 使用字典类型,至少有一个value 为 Series 类型(即 pd.Series),可以出事化行数, Series指定index
    df2 = pd.DataFrame({"A": pd.Timestamp("20161010"), "B": pd.Series(1, index=list(range(7))), "C": pd.Series(1, index=list(range(4)))})
    # 使用 pd.Series 初始化
    df2 = pd.DataFrame(pd.Series([1, 2]))  #一列两行
    df2 = pd.DataFrame(pd.Series({"d": "b", "dd": "aa"}))  # map 两列两行

    df2 = pd.DataFrame(pd.Series({"d": "b", "dd": "aa", "da": "aa"}))  # 两列 三行

    # df2 = pd.DataFrame(pd.Series([["a","b"],["c","bd"]]))  #错误 不支持

    #错误的
    # df2 = pd.DataFrame(pd.Series([{"d": "b", "dd": "aa"}, {"d": "b", "dd": "aa"}]))
    df2 = pd.DataFrame(pd.Series(3, index=list(range(7))))

    #察看 df 的数据格式
    df.dtypes

    print df2.head(2)  #察看前几行数据 ,默认前五行
    print df2.tail(2)  #察看最后几行数据 ,默认后五行

    print df2.index  #察看索引
    print df2.columns.tolist()  #察看 列名
    print df2.values  #察看 数值
    print df2.describe()  #察看 描述性统计

    print df2.T  # 转制数据

    print df2
    # 在pandas中用函数 isnull notnull 来检测数据丢失
    df2.isnull()
    pd.isnull(df2)
    pd.notnull(df2)


def test2():
    '''
    提取数据
    :return:
    '''
    def test_sub1():
        '''
        提取数据
         loc /  at  是按 index 和 columns  来提取 数据的,

          df直接 提取数据
            1、["A", "B"]  可以提取列
            2、1:3  或 "20161201":"20161203" 可以提取行
            不能一次性提取行列
        :return:
        '''
        dates = pd.date_range("20161201", periods=7)
        df = pd.DataFrame(np.random.randn(7, 5), index=dates, columns=list("ABCDE"))

        print df["A"]  #选择 A 列  返回 Series
        print df[["A", "B"]]  # 选择 A 列  返回 Series
        print df[1:3]  #选择 大于等于1 小于 3 的行 (通过 rownumber 过滤)
        print df["20161201":"20161203"]  #可以使用index 的值 过滤

        # 获取数据 返回 Series ,
        # loc 中行数据是通过 index 的值来获取的(是大于等于,小于等于),不能通过 rownumber 获取
        df.loc["20161203"]  #取某一行的数据
        df.loc[dates[0]]   #取某一行的数据

        # ix  索引时，选取的是列(列行); 切片时，选取的是行(行列)
        # ix 对象可以接受两套切片，分别为行（axis=0）和列（axis=1）的方向
        df.ix[1]  # #取某一行的数据    切片
        df.ix[1, :-1]  # 切片

        df.ix["2016-12-02": "2016-12-07"]  # 索引,  选择的 是 行
        df.ix["2016-12-02": "2016-12-07", "A": "C"]
        df.ix["2016-12-02", "A"]

        df.loc[:, ["A", "B"]]  # 取 哪几列数据
        df.loc[dates[0]:dates[3], ["A", "B"]]  # 取 几行几列数据

        #获取某个数据的值
        df.loc[dates[0], ["A"]]  # 返回 Series
        df.loc[dates[0], "A"]  # 返回 数据
        df.loc[dates[0]:dates[3], "A"]  # 返回 Series

        #at 专门获取数据, 如果 想让返回 Series ,则 at方法会报错
        df.at[dates[0], "A"]

    def test_sub2():
        '''
        提取数据
        iloc /  iat  是按 下标索引来提取行列数据的
        :return:
        '''
        dates = pd.date_range("20161201", periods=8)
        df = pd.DataFrame(np.random.randn(8, 6), index=dates, columns=list("ABCDEF"))
        df.icol(3)  # 已废弃
        #  切片获取数据 ,行列 iloc 会后值 Series
        df.iloc[:, 3]  #获取第三列的数据
        df.iloc[1:2, 3]
        df.iloc[[1, 3], [2, 5]]  #提取不连续的行列
        df.iloc[[1, 3], :]  # 提取所有列

        #  提取某个数值
        df.iloc[2, 3]  # 提取第二行第三列的值
        df.iat[2, 3]  # 提取第二行第三列的值  效率更高


def test3():
    '''
    筛选数据
    :return:
    '''
    dates = pd.date_range("20161201", periods=12)
    df = pd.DataFrame(np.random.randn(12, 6), index=dates, columns=list("ABCDEF"))

    df[df.D > 0]  # 筛选 D 列 大于 0 的数据
    df[(df.D > 0) & (df.C < 0)]  # 用 & 实现且 , | 实现 或
    df[(df.D > 0) | (df.C < 0)]

    df[["A", "B"]][(df.D > 0) & (df.C < 0)]  # 筛选结果只取 A 、 B 两列

    index = (df.D > 0) & (df.C < 0)  # 通过一个布尔索引 过滤
    df[index]

    # 使用isin方法来筛选特定的值
    alist = [-1, 1]
    df["A"].isin(alist)
    df["A"] = df["A"].astype("int")
    # df.loc["2016-12-12", "A"]
    df[df["A"].isin(alist)]


path_str = "/Users/guoyuanpei/workspace/pworkspace/mpintranettool/test2.csv"
def test4():
    '''
    读取 csv 数据
    统计数据
    输出柱状图
    :return:
    '''
    # path_str = "/Users/guoyuanpei/workspace/pworkspace/mpintranettool/tao_price_2.txt"

    df = pd.read_csv(path_str, sep="\t", encoding="utf-8")
    import json
    # json.loads()
    df.iloc[3, 3]
    df["5720"].value_counts()
    counts = df.iloc[:, 3].value_counts()  # 返回 Series
    plt = counts.plot(kind="bar").get_figure()
    plt.savefig("/Users/guoyuanpei/workspace/pworkspace/mpintranettool/plot.png")



def test5():
    '''
    统计分析
    求 占比
    :return:
    '''
    df = pd.read_csv(path_str, sep="\t", encoding="utf-8")
    good = df[df.iloc[:, 7] > 80]  # 筛选大于 50 的列
    g1 = good.iloc[:, 7].value_counts()
    g2 = df.iloc[:, 7].value_counts()
    c2 = good.iloc[:, 3].value_counts()
    per = g1 / g2


def test6():
    '''
    数据分组  group by
    :return:
    '''
    def test_sub1():
        '''
        数据分组
        group by
        计算 ......
        :return:
        '''
        df = pd.DataFrame({"A": ["foo", "bar", "foo", "bar", "foo", "bar", "foo", "foo"], "B": ["one", "one", "two", "three", "two", "two", "one", "three"], "C": np.random.randn(8), "D": np.random.randn(8)})
        grouped = df.groupby("A")
        grouped.first()  # 打印第一组数据
        grouped.size()
        grouped.last()

        grouped_t = df.groupby(["A", "B"])  # 两列以上进行分组

        # 根据列来分组
        def get_type(letter):
            if letter.lower() in "abem":
                return "vowel"
            else:
                return "consonant"

        grouped = df.groupby(get_type, axis=1)
        grouped.first()


    def test_sub2():
        '''
        multi index用法
        多索引的使用
        multi index 在分组中的使用
        :return:
        '''
        colors = np.random.choice(["red", "green"], size=10)
        foods = np.random.choice(["eggs", "ham"], size=10)

        index = pd.MultiIndex.from_arrays([colors, foods], names=["color", "food"])
        df = pd.DataFrame(np.random.randn(10, 2), index=index)

        # 使用索引来筛选数据
        df.query("color=='red'")  # 报错 需要安装 numexpr

        # 在分组中使用索引
        grouped = df.groupby(level="food")  # ? 如果删除索引名称, 该如何来进行 分组
        grouped.sum()

        # 删除或更改索引名称
        df.index.names = [None, None]
        df.query("ilevel_0=='red'")  # 只能通过 ilevel_0 来访问

        grouped = df.groupby(level=1)  # 只能通过索引号来访问 0 、 1
        grouped.sum()


def test7():
    '''
    group by 选择列 和迭代
    :return:
    '''
    colors = np.random.choice(["red", "green"], size=10)
    foods = np.random.choice(["eggs", "has"], size=10)

    index = pd.MultiIndex.from_arrays([colors, foods], names=["color", "food"])
    df = pd.DataFrame(np.random.randn(10, 2), index=index)
    df.columns = ["a", "b"]

    grouped = df.groupby(level="color")
    grouped_a = grouped["a"]
    grouped_a.sum()

    for name, group in grouped:
        print name
        print group
        print "================"

    for name, group in df.groupby(level=["color", "food"]):
        print name
        print group
        print "================"


def test8():
    '''
    group by  aggregate
    :return:
    '''
    colors = np.random.choice(["red", "green"], size=10)
    foods = np.random.choice(["eggs", "has"], size=10)

    index = pd.MultiIndex.from_arrays([colors, foods], names=["color", "food"])
    df = pd.DataFrame(np.random.randn(10, 2), index=index, columns=["a", "b"])

    grouped = df.groupby(level=["color", "food"])

    grouped.size()  # 返回个组的数据个数

    grouped.describe()  # 返回对各组数据进行的描述统计

    grouped.aggregate(np.sum)  # 分组求和

    grouped.aggregate(np.sum).reset_index()  # 将两个 列索引 转化 为 列变量

    df.groupby(level=["color", "food"], as_index=False).sum()  # 在分组时 结果中去掉 列索引


def test9():
    '''
    transformation 标准化数据
    数据转化
    ? 不理解使用
    mean : 平均值
    std : 标准差
    :return:
    '''
    index = pd.date_range("1/1/2014", periods=100)

    ts = pd.Series(np.random.normal(0.5, 2, 100), index=index)  # np.random.normal 0.5 : 均值 ; 2: 标准差; 100 : 返回个数

    key = lambda x: x.month
    zscore = lambda x: (x - x.mean()) / x.std()
    transformed = ts.groupby(key).transform(zscore)

    transformed.groupby(key).mean()
    transformed.groupby(key).std()


def test10():
    '''
    agg 分组
    :return:
    '''
    colors = np.random.choice(["red", "green"], size=10)
    foods = np.random.choice(["eggs", "has"], size=10)

    index = pd.MultiIndex.from_arrays([colors, foods], names=["color", "food"])
    df = pd.DataFrame(np.random.randn(10, 2), index=index, columns=["a", "b"])

    grouped = df.groupby(level="color")

    grouped.agg([np.sum, np.mean, np.std])
    grouped["a"].agg([np.sum, np.mean, np.std])
    grouped["a"].agg({"sum result": np.sum, "mean result": np.mean, "std result": np.std})
    grouped["a"].agg({"lambda_a": lambda x: np.mean(abs(x))})
    grouped["a"].agg({"C": "sum", "D": "std"})


def test11():
    '''
    对分组字断进行 计算
    分组 默认传入的是 索引列
    :return:
    '''
    index = pd.date_range("1/1/2014", periods=100)
    ts = pd.Series(np.random.normal(0.5, 2, 100), index=index)

    key = lambda x: x.month
    grouped = ts.groupby(key)  # 按索引分组

    ##############################################
    date = pd.date_range("1/1/2014", periods=100)
    data = np.random.normal(0.5, 2, 100)
    df = pd.DataFrame({"date": date, "data": data})

    df.groupby(df["date"].apply(lambda x: x.month)).first()  # 按 数据分组

    # 设置索引
    df = df.set_index("date")  # 设置索引
    grouped = df.groupby(key)  # 默认转入的 是 索引 分组

    date_stngs = ("20081220", "1/1/2008", "2008-12-22", "2008-12-23")
    a = pd.Series([pd.to_datetime(_) for _ in date_stngs])  # 数据格式转化


def test12():
    '''
    移动 、 复制、删除列
    :return:
    '''
    colors = np.random.choice(["red", "green"], size=10)
    foods = np.random.choice(["eggs", "has"], size=10)

    index = pd.MultiIndex.from_arrays([colors, foods], names=["color", "food"])
    df = pd.DataFrame(np.random.randn(10, 2), index=index, columns=["a", "b"])

    df["c"] = pd.Series(np.random.randn(10), index=df.index)  # 增加列

    df.insert(1, "e", df["a"])  # 插入列

    del df["e"]  # 永久删除一列
    df2 = df.drop(["c"], axis=1)  # 删除一列,返回新数据

    # 移动列
    b = df.pop("b")
    df.insert(0, "b", b)


def test13():
    '''
    字符串操作
    :return:
    '''
    s = pd.Series(list("ABCDEF"))

    s.str.lower()  # 将数据转为 小写
    s.str.upper()  # 将数据转为 大写
    s.str.len()  # 获取字符串长度

    s2 = pd.Series(["a_b_c", "c_a_b", np.nan, "f_g_h"])  # 切割字符串
    s2.str.split("_")

    s2.str.split("_").str.get(1)  # 如果 某列是一个 list  可以用 str.get(index) 获取该列的第几个元素

    s2.str.replace("a|b$", "x", case=False)  # 字符串替换

    #################################### 字符串提取 ####################################################
    s = pd.Series(["a1", "a2", "b1", "b2", "c3", "c"])

    s.str.extract("[ab](\d)", expand=False)  # extract 第一个参数是正则表达式, 括号表示要提取的部分

    s.str.extract("([abc])(\d)", expand=False)  # extract 第一个参数是正则表达式, 括号表示要提取的部分 ; 提取多个数据

    s.str.extract("([abc])(\d)?", expand=False)  # extract 第一个参数是正则表达式, 括号表示要提取的部分 ; 提取多个数据

    s.str.extract("(?P<letter>[abc])(?P<digit>\d)", expand=False)  # extract 第一个参数是正则表达式, 括号表示要提取的部分 ; 提取多个数据; 输出的包含变量名

    #################################### 字符串匹配 ####################################################
    s = pd.Series(["1a", "A2", "b1", "ab2", "c3", "abd", "a2c", np.nan, "a1b"])

    s.str.contains(r"[a-z][0-9]", na=False)  # na参数来规定出现NaN数据的时候匹配成True还是False

    s.str.match(r"[a-z][0-9]", as_indexer=False)  # 严格匹配  从开头匹配

    s.str.startswith("a", na=False)
    s.str.contains("^a", na=False)

    s.str.endswith("a", na=False)
    s.str.contains("a$", na=False)


def test14():
    '''
    连接数据库
    :return:
    '''
    import psycopg2
    conn = psycopg2.connect(database="mp_portal",
                                 user="elengjing",
                                 password="Marcpoint2016",
                                 host="192.168.110.11",
                                 port="5432")
    df = pd.read_sql("select id, name, platform   from shop limit 10", conn, index_col="id")
    print "==========="
    print df["platform"]

    return df


def test14():
    '''
    广播
    :return:
    '''
    df = pd.DataFrame({"one": np.random.randn(4)}, index=list("abcd"))
    # df = pd.DataFrame({"one": pd.Series(np.random.randn(4), index=list("abcd"))})
    df["two"] = 1
    df["thr"] = 2

    row = df.ix[1]
    column = df["two"]
    df.sub(row, axis="columns")  # 将df中每一行与row做减法 通过参数axis可指定广播的维度，axis=1或者axis=‘column’

    df2 = pd.DataFrame([row for i in range(4)], index=["a", "b", "c", "d"])

    df2 -df  # 矩阵减法


def test15():
    '''
    值填充 及 替换
    带有确实只的计算
    :return:
    '''
    df1 = pd.DataFrame(np.random.randn(5, 3), index=list("abcde"), columns=["one", "two", "three"])
    df1.ix[3, :-1] = np.nan

    df2 = pd.DataFrame(np.random.randn(5, 3), index=list("abcde"), columns=["one", "two", "three"])

    df1 + df2  # 在描述性统计中，Nan都是作为0进行运算

    # 填充缺失值
    df1.fillna(0)  # 用0 填充
    df1.fillna("missing")  # 用 字符串 填充
    df1.fillna(method="pad")  # 用 前一个数据代替 NaN   method='pad'
    df1.fillna(method="bfill", limit=1)  # 用 后一个数据代替 NaN   method='bfill'
    df1.fillna(df1.mean())  # 用 平均数 代替 NaN
    df1.fillna(df1.mean()["one":"two"])  # 用 那些列 的 平均数 代替 NaN

    # 删除缺失数据
    df1.dropna(axis=0)  # 删除有缺失值的 行
    df1.dropna(axis=1)  # 删除有缺失值的 列

    #插值法填补缺失值
    df1.interpolate()  # 假设函数是直线形式

    df1.index = [1, 2, 3, 4, 5]
    df1.interpolate(method="values")  # 假如index是数字，我们还可以根据数字来进行插值;索引的数值实际上就是用于估计y的x

    # 如果index 是时间
    df1.index = pd.date_range("20161014", periods=5)
    df1.interpolate(method="time")

    # 值替换
    ser = pd.Series([0, 1, 1, 3, 4, 5, 1, 2, 3])

    ser.replace(0, 6)  # 0 替换成 6
    ser.replace([0, 1, 1, 3, 4, 5], [5, 4, 3, 2, 1, 0])  # 列表到列表的替换
    ser.replace({1: 11, 2: 22})  # 字典映射 替换

    df = pd.DataFrame({"a": [0, 11, 22, 3, 4, 2], "b": [5, 62, 2, 2, 8, 9]})

    df.replace({"a": 0, "b": 5}, np.nan)  # 将多个列中不同的值都要替换为一个相同的值

    df["a"].replace([1, 2, 3], method="pad")  # 插值法同样可以用于替换数值，只要使用参数method即可


def test16():
    '''
    散点图 和 抖动图
    散点图 添加 趋势线
    :return:
    '''
    epath = "/Users/guoyuanpei/workspace/pworkspace/mpintranettool/test.xls"
    df = pd.read_excel(epath, header=3)

    # 抖动图
    plt = df.plot(kind="scatter", x="t1", y="t2").get_figure()
    plt.savefig("xls.png")

    # 散点图
    def jitter(series, factor):
        z = float(series.max()) - float(series.min())
        a = float(factor) * z / 50
        return series.apply(lambda x: x + np.random.uniform(-a, a))

    df2 = df
    df2["t1"] = jitter(df["t1"], 100)
    df2["t2"] = jitter(df["t2"], 100)

    plt = df2.plot(kind="scatter", x="t1", y="t2", alpha=.5).get_figure()
    plt.savefig("xls2.png")

    # 散点图 添加 曲线
    from statsmodels.formula.api import ols  # 需要安装 statsmodels  patsy
    import matplotlib.pyplot as plt

    lm = ols("t1~t2", df).fit()  # 使用ols进行回归拟合，实际上是建立了 t2 为自变量的线性回归方程
    plt.plot(df["t2"], df["t1"], "ob")  # 绘制散点图
    plt.plot(df["t2"], lm.fittedvalues, "r", linewidth=2)  # 绘制拟和直线
    plt.show()  # 察看绘制出来的图


def test17():
    '''
    柱形图
    等 其他图

    柱形图一般适用于离散数据，而直方图更多用于连续数据
    :return:
    '''
    df = pd.DataFrame(np.random.randn(10, 4), columns=list("abcd"))

    # 设置柱形图的样式
    pd.set_option("mpl_style", "default")

    plt = df.plot(kind="bar").get_figure()
    plt.savefig("bar.png")


def test18():
    '''
    1、 索引对齐  运算
    2、 函数应用和映射
    3、 排序
    4、 排名
    5、 统计方法
    :return:
    '''
    foo = pd.Series({'a': 1, 'b': 2})
    bar = pd.Series({'b': 3, 'd': 4})

    foo + bar  # 取并集 相加,  不匹配的为 NAN

    foo.add(bar, fill_value=0)  # 不匹配 的 填 0  然后相加
    #  sub(), div(), mul()

    #  函数应用和映射  当希望将函数应用到 DataFrame 对象的某一行或列时，可以使用 .apply(func, axis=0, args=(), **kwds) 方法
    f = lambda x: x.max() - x.min()
    data = {'state': ['Ohino', 'Ohino', 'Ohino', 'Nevada', 'Nevada'], 'year': [2000, 2004, 2002, 2005, 2004],
            'pop': [1.5, 1.7, 3.6, 2.4, 2.9]}

    df = pd.DataFrame(data)
    df[["year", "pop"]].apply(f, axis=0)   # axis 维度 0 : 列   ;  1  : 行  默认为 0

    df["year"].apply(f, axis=0)

    #  排序
    df.sort_index(ascending=False)  # 对 DataFrame 的按索引排序
    df.sort_values(ascending=False, by=["year"])  # 对 DataFrame 的 值 进行排序

    df["year"].order(ascending=False)  # 对 Series 的值 进行排序
    df["year"].sort_index(ascending=False)  # 对 Series 的索引 进行排序

    # 排名
    # 排名（Series.rank(method='average', ascending=True)）
    # 排名的作用与排序的不同之处在于，他会把对象的 values 替换成名次（从 1 到 n）   平级 处理策略  method 参数，他有四个值可选：average, min, max, first
    ser = pd.Series([3, 2, 0, 3], index=list('abcd'))

    ser.rank(method="first")
    ser.rank(method="min")
    ser.rank(method="max")
    ser.rank(method="average")
    ser.rank()  # 默认是  average

    #  DataFrame 的 .rank(axis=0, method='average', ascending=True)
    df.rank(axis=0, method='average')

    # 统计方法


def test19():
    '''
    数据合并
    1、 merge 数据看风格的合并
        merge(left,right,how='inner',on=None,left_on=None,right_on=None,left_index=False,right_index=False,sort=False,suffixes=('_x','_y'),copy=True)
        on=None 指定连接的列名，若两列希望连接的列名不一样，可以通过left_on和right_on 来具体指定
        how=’inner’,参数指的是左右两个表主键那一列中存在不重合的行时，取结果的方式：inner表示交集，outer 表示并集，left 和right 表示取某一边。
    2、 concat
     concat(objs,axis=0,join='outer',join_axes=None,ignore_index=False,keys=None,levels=None,names=None,verigy_integrity=False)
        objs 是需要拼接的对象集合，一般为列表或者字典
        axis=0 是行拼接，拼接之后行数增加，列数也根据join来定，join=’outer’时，列数是两表并集。同理join=’inner’,列数是两表交集。

    3、 append
        DataFrame.append(other, ignore_index=False, verify_integrity=False)
            other：DataFrame or Series/dict-like，或者上述类型的列表，就是要添加的数据
            verify_integrity : boolean，默认值为False，在设置为True时，index标签重复时会报错。

    4、 高效的添加一行数据
    :return:
    '''
    df1 = pd.DataFrame([[1, 2, 3], [5, 6, 7], [3, 9, 0], [8, 0, 3]], columns=['x1', 'x2', 'x3'])
    df2 = pd.DataFrame([[1, 2], [4, 6], [3, 9]], columns=['x1', 'x4'])

    pd.merge(df1, df2, how='left', on='x1')

    df2.columns = ["a1", "a2"]
    pd.merge(df1, df2, how="left", left_on="x1", right_on="a1")

    # cancat 拼接
    pd.concat([df1, df2])
    # 等价于
    df1.append(df2)

    # 行拼接
    pd.concat([df1, df2], axis=1)
    #等价于
    pd.merge(df1, df2, left_index=True, right_index=True, how='outer')

    # append
    df = pd.DataFrame([[5, 6], [7, 8]], columns=['A', 'B'])
    df2 = pd.DataFrame([[1, 2], [3, 4]], columns=['C', 'B'])
    df.append(df2)

    df.append(df2, ignore_index=True)  # 重新生成新的 index

    ser = pd.Series([5, 6])
    df.append(ser, ignore_index=True)

    ser = pd.Series([5, 6], name='P')  # append Series 时, 会将 Series 转换成 行 来添加, 该Series 必须有name 来所谓新的index
    df.append(ser)

    # 高效的添加一行数据
    df.loc[len(df.index)+1] = {'A': 1, 'B': 2}
    df.shape[1]

    pd.concat([df, pd.Series({'A': 0, 'B': 1})])
    pd.concat([df, pd.DataFrame(pd.Series({'A': 0, 'B': 0})).T])

    pd.concat([df, pd.DataFrame([["1", "1"]], columns=df.columns)])
    # pd.concat([df, pd.DataFrame([["1", "1"]])])

    pd.concat([df, pd.DataFrame({'A': [0], 'B': [0]})])
    pd.concat([df, pd.DataFrame({'A': 0, 'B': 0}, index=[1])])
    pd.concat()

    df = pd.DataFrame([{"a": "1", "b": "2", "c": "4", "d": "6"}, {"a": "3", "b": "3", "c": "4", "d": "6"}], columns=["a", "b", "c", "d"])
    def pp(ser):
        return ser.apply(lambda x: int(x) * 10)

    df[["a", "b"]] = df[["a", "b"]].apply(pp)

    df1 = df.copy()
    df3 = df.merge(df1, how="left", on=["a", "b"])
    df3.fillna(0).drop_duplicates()

    df.values.tolist()

    [dict(zip(df.columns, row)) for row in df.values.tolist()]
    df.to_dict("records")
    # to_dict 的使用
    '''
    DataFrame.to_dict(orient='dict')
        orient : str {‘dict’, ‘list’, ‘series’, ‘split’, ‘records’, ‘index’}
            Determines the type of the values of the dictionary.
            dict (default) : dict like {column -> {index -> value}}
            list : dict like {column -> [values]}
            series : dict like {column -> Series(values)}
            split : dict like {index -> [index], columns -> [columns], data -> [values]}
            records : list like [{column -> value}, ... , {column -> value}]
            index : dict like {index -> {column -> value}}
            New in version 0.17.0.
            Abbreviations are allowed. s indicates series and sp indicates split.
    '''



    def test18():
        '''

        :return:
        '''

        data = 'col1,col2,col3\na,b,1\na,b,2\nc,d,3'

        df = pd.read_csv(StringIO(data))
        df.dtypes
        df = pd.read_csv(StringIO(data), dtype='category')
        df.dtypes


dates = pd.date_range("20161201", periods=7)
pd = pd.DataFrame([
    {"a": "1", "b": "2", "c": "3"},
    {"a": "2", "b": "5", "c": "11"},
    {"a": "3", "b": "2", "c": "30"},
    {"a": "4", "b": "5", "c": "3"},
])

ss = {"1": "11111", "2": "2222", "3": "3333", "4":"4444"}
pd["d"] = pd.apply(lambda x: int(x["a"]) * int(x["b"]), axis=1)

print pd.columns