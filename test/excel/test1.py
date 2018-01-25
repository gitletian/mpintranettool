# coding: utf-8
# __author__: ""
from __future__ import print_function, unicode_literals, division
import xlwt
import copy
import pdb

platform = [
    dict(platformid="2011", platfrom_name="宝宝树"),
    dict(platformid="2012", platfrom_name="宝宝树1"),
    dict(platformid="2013", platfrom_name="宝宝树2"),
    dict(platformid="2014", platfrom_name="宝宝树3"),
    dict(platformid="2015", platfrom_name="宝宝树4"),
    dict(platformid="2016", platfrom_name="宝宝树5"),
]


subject = [
    dict(subject="subject1", radio=0.12),
    dict(subject="subject2", radio=0.13),
    dict(subject="subject3", radio=0.14),
    dict(subject="subject4", radio=0.15),
    dict(subject="subject5", radio=0.16),
    dict(subject="subject6", radio=0.17),
    dict(subject="subject7", radio=0.18),
]


def writexcle(datas, sheets):
    wk = xlwt.Workbook(encoding="utf8")

    for sheet in sheets:
        columns = sheet["columns"]
        columns_key = sheet["columns_key"]
        sheetname = sheet["sheetname"]

        data = copy.deepcopy(datas[sheet["sheet_key"]])
        data.insert(0, dict(zip(columns_key, columns)))

        ws = wk.add_sheet(sheetname=sheetname)
        for i in range(len(data)):
            row = data[i]
            for j in range(len(columns_key)):
                ws.write(i, j, row[columns_key[j]])

    wk.save('Excel_Workbook.xls')


sheets = [
    dict(sheetname="平台数据", sheet_key="platform", columns=["平台id", "平台名称"], columns_key=["platformid", "platfrom_name"]),
    dict(sheetname="板块数据", sheet_key="subject", columns=["板块名称", "贡献率"], columns_key=["subject", "radio"])
]

datas = dict(platform=platform, subject=subject)

writexcle(datas, sheets)








