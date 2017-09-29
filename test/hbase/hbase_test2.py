# coding: utf-8
# __author__: ""
from __future__ import unicode_literals
from itertools import groupby

# data = [
#    {"test1": "1111", "test2": "bbb1", "test3": "ccc1", "test4": "ddd1"},
#    {"test1": "1111", "test2": "bbb", "test3": "ccc2", "test4": "ddd2"},
#    {"test1": "2222", "test2": "bbb1", "test3": "ccc3", "test4": "ddd3"},
#    {"test1": "5555", "test2": "bbb1", "test3": "ccc4", "test4": "ddd4"},
#    {"test1": "3333", "test2": "bbb", "test3": "ccc5", "test4": "ddd5"},
#    {"test1": "333", "test2": "bbb", "test3": "ccc6", "test4": "ddd6"},
#    {"test1": "555", "test2": "bbb1", "test3": "ccc7", "test4": "ddd7"},
#    {"test1": "555", "test2": "bbb", "test3": "ccc8", "test4": "ddd8"},
# ]

data = [
    {
      "date_range": "2015-01-26~2015-02-01",
      "name": "1111",
      "salesamt": "276.71"
    },
    {
      "date_range": "2015-01-26~2015-02-01",
      "name": "22222",
      "salesamt": "57.89"
    },
    {
      "date_range": "2015-01-26~2015-02-01",
      "name": "3333",
      "salesamt": "213.34"
    },
    {
      "date_range": "2015-01-26~2015-02-01",
      "name": "33333",
      "salesamt": "0.00"
    },
    {
      "date_range": "2015-01-19~2015-01-25",
      "name": "4444",
      "salesamt": "62.50"
    },
    {
      "date_range": "2015-01-19~2015-01-25",
      "name": "韩版",
      "salesamt": "80.80"
    },
    {
      "date_range": "2015-01-19~2015-01-25",
      "name": "学院",
      "salesamt": "0.00"
    },
    {
      "date_range": "2015-01-19~2015-01-25",
      "name": "运动休闲",
      "salesamt": "0.00"
    },
    {
      "date_range": "2015-02-16~2015-02-22",
      "name": "通勤",
      "salesamt": "-90.07"
    },
    {
      "date_range": "2015-02-16~2015-02-22",
      "name": "韩版",
      "salesamt": "-81.96"
    },
    {
      "date_range": "2015-02-16~2015-02-22",
      "name": "学院",
      "salesamt": "-86.49"
    },
    {
      "date_range": "2015-02-16~2015-02-22",
      "name": "运动休闲",
      "salesamt": "-66.67"
    },
    {
      "date_range": "2015-02-09~2015-02-15",
      "name": "通勤",
      "salesamt": "-17.59"
    },
    {
      "date_range": "2015-02-09~2015-02-15",
      "name": "韩版",
      "salesamt": "-55.10"
    },
    {
      "date_range": "2015-02-09~2015-02-15",
      "name": "学院",
      "salesamt": "-59.09"
    },
    {
      "date_range": "2015-02-09~2015-02-15",
      "name": "运动休闲",
      "salesamt": "-50.00"
    },
    {
      "date_range": "2015-02-02~2015-02-08",
      "name": "通勤",
      "salesamt": "7.99"
    },
    {
      "date_range": "2015-02-02~2015-02-08",
      "name": "韩版",
      "salesamt": "-16.18"
    },
    {
      "date_range": "2015-02-02~2015-02-08",
      "name": "学院",
      "salesamt": "-28.60"
    },
    {
      "date_range": "2015-02-02~2015-02-08",
      "name": "运动休闲",
      "salesamt": "0.00"
    }
  ]

def dict_group(record_list, keys, values):
   result = []
   record_list = sorted(record_list, key=lambda _: {key: _[key] for key in keys if _.has_key(key)})
   for key, rows in groupby(record_list, key=lambda _: {key: _[key] for key in keys if _.has_key(key)}):
      # print key
      # print list(rows)
      if type(values) == tuple:
         key[values[0]] = [{_: row[_] for _ in values[1] if row.has_key(_)} for row in rows]

      elif type(values) == list:
         rows = list(rows)
         for _ in values:
            key[_[0]] = [v[_[1]] for v in rows if v.has_key(_[1])]

      result.append(key)

   print result

# dict_group(data, ["test1", "test2"], ("newname", ["test3", "test4"]))
# dict_group(data, ["test1"], [("test3_array", "test3"), ("test4_array", "test4")])

dict_group(data, ["name"], [("date_range_1", "date_range"), ("salesamt_1", "salesamt")])
















