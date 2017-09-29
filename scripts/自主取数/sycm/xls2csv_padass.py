#coding: utf-8
from __future__ import unicode_literals
import sys
reload(sys)
sys.setdefaultencoding("utf-8")
import pandas as pd
import os
import glob
import re
import datetime
import pdb


class Xls2csv:
    def __init__(self, in_path, out_path, data_type, shopid, shopname):
        """
        生意参谋数据 xls转 csv
        :param in_path:输入路径
        :param out_path:输出路径
        :param data_type:数据类型 1:日数据; 2: 月数据
        :return:
        """
        self.in_path = in_path
        self.out_path = out_path
        self.data_type = data_type
        self.shopid = shopid
        self.shopname = shopname

    def read_xls(self, filename):
        """
        生意参谋数据 xls转 csv
        :param filename:要转换的文件
        :return:
        """
        df = pd.read_excel(filename, header=3)
        if not os.path.exists(self.out_path):
            os.mkdir(self.out_path)

        filename = os.path.splitext(os.path.basename(filename))[0]
        if self.data_type == 1:
            # 转换日数据的 datarange 的生成
            datarange = re.split("-2017-|-2016-", filename)[0]

        if self.data_type == 2:
            # 转换月数据的 datarange 的生成
            datarange = re.match(r".*?(\d+)", filename).group(1)
            datarange = datetime.datetime.strptime(datarange, "%Y%M").strftime("%Y-%M")

        df["datarange"] = datarange
        df["datatype"] = self.data_type
        df["shopid"] = self.shopid
        df["shopname"] = self.shopname
        print datarange
        df["商品id"] = df["商品id"].astype("int")

        filename += ".csv"
        df.to_csv(os.path.join(self.out_path, filename), index=False, header=False, sep=str("\t"))

    def itertor_dic(self):
        """
        :return:
        """
        for file_name in glob.glob(os.path.join(self.in_path, "*.xls")):
            self.read_xls(file_name)


if __name__ == '__main__':

    xls2csv = Xls2csv("/Users/guoyuanpei/Downloads/sycm0503/klder", "/Users/guoyuanpei/Downloads/sycm0503/klder_csv", 1, 66098091, "珂莱蒂尔旗舰店")
    xls2csv.itertor_dic()
    xls2csv = Xls2csv("/Users/guoyuanpei/Downloads/sycm0503/lily", "/Users/guoyuanpei/Downloads/sycm0503/lily_csv", 1, 73401272, "lily官方旗舰店")
    xls2csv.itertor_dic()
    xls2csv = Xls2csv("/Users/guoyuanpei/Downloads/sycm0503/naersi", "/Users/guoyuanpei/Downloads/sycm0503/naersi_csv", 1, 69302618, "naersi旗舰店")
    xls2csv.itertor_dic()


