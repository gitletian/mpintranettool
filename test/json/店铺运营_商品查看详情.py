# coding: utf-8
# __author__: ""
####################################### 店铺运营 -> 商品-查看详情 #######################################
# 请求(默认)参数
{
    "interval": [
        {"name": "start_date", "value": "2017-01-02"},
        {"name": "end_date", "value": "2017-05-02"},
        {"name": "date_type", "value": "1"}
    ],
    "item": "538919327302",
    "shop": "58501945"
}


### 商品-查看详情-汇总
# 返回内容

{
    "shop_id": 1,
    "shop_name": "ZARA官方旗舰店",
    "category_id": 1,
    "category_name": "衬衫",
    "item_id": 444,
    "item_name": "ZARA2017",
    "item_pic": "",
    "qty": "42",
    "qty_rank": "3",
    "qty_rank_drift": "-3",
    "sales": "34444",
    "sales_rank": "2",
    "sales_rank_drift": "1",
    "spu": "34444",
    "spu_rank": "2",
    "spu_rank_drift": "1",
}


### 商品-查看详情-同位竞品榜单
# 返回内容

[
    {"rank": "1", "item_id": "538919327302", "item_name": "ROEM罗燕 依恋旗下---34903（吊牌698）深蓝色/杏色甜美线毛衣女", "item_pic": "", "item_url": "https://item.taobao.com/item.htm?id=538919327302", "shop_name": "韩都衣舍旗舰店", "shop_id": "58501945", "qty": "234", "listed_date": "2014-01-11", "stock": "300", "price": "10", "favourite": "20" , "qty_link_increase_ratio": "10", "is_follow": False},
    {"rank": "2", "item_id": "532844922420", "item_name": "2016秋冬新款棉衣女长袖短款内胆大码加厚修身中老年轻薄保暖内衣", "item_pic": "", "item_url": "https://item.taobao.com/item.htm?id=538919327302", "shop_name": "韩都衣舍旗舰店", "shop_id": "103314362", "qty": "234", "listed_date": "2014-01-11", "stock": "300", "price": "30", "favourite": "20", "qty_link_increase_ratio": "10", "is_follow": False},
    {"rank": "3", "item_id": "538919327302", "item_name": "ROEM罗燕 依恋旗下---34903（吊牌698）深蓝色/杏色甜美线毛衣女", "item_pic": "", "item_url": "https://item.taobao.com/item.htm?id=538919327302", "shop_name": "韩都衣舍旗舰店", "shop_id": "58501945", "qty": "234", "listed_date": "2014-01-11", "stock": "300", "price": "44", "favourite": "20", "qty_link_increase_ratio": "10", "is_follow": False},
    {"rank": "4", "item_id": "537361444972", "item_name": "韩版百搭钉珠镶钻领冰丝针织无袖上衣修身显瘦吊带背心女打底衫夏", "item_pic": "", "item_url": "https://item.taobao.com/item.htm?id=538919327302", "shop_name": "韩都衣舍旗舰店", "shop_id": "112067908", "qty": "234", "listed_date": "2014-01-11", "stock": "300", "price": "30", "favourite": "20", "qty_link_increase_ratio": "10", "is_follow": False},
    {"rank": "5", "item_id": "545373623514", "item_name": "2016春装新款气质小香风性感修身名媛水钻钉珠连衣裙acp", "item_pic": "", "item_url": "https://item.taobao.com/item.htm?id=538919327302", "shop_name": "韩都衣舍旗舰店", "shop_id": "59239838", "qty": "234", "listed_date": "2014-01-11", "stock": "300", "price": "30", "favourite": "20", "qty_link_increase_ratio": "10", "is_follow": False},
    {"rank": "6", "item_id": "544843732679", "item_name": "玮玛2017春装新款女装复古文艺淑女娃娃领纯棉修身印花连衣裙182H", "item_pic": "", "item_url": "https://item.taobao.com/item.htm?id=538919327302", "shop_name": "韩都衣舍旗舰店", "shop_id": "59084964", "qty": "234", "listed_date": "2014-01-11", "stock": "300", "price": "10", "favourite": "20", "qty_link_increase_ratio": "10", "is_follow": False}
]


### 商品-查看详情-销售趋势
# 返回内容
[
    {"item_id": "537361444972", "item_name": "R色/杏色甜美线毛衣女", "qty_increase_ratio": "30", "date_range": "2017-01-02"},
    {"item_id": "538919327302", "item_name": "同位竞品1", "qty_increase_ratio": "30", "date_range": "2017-01-02"},
    {"item_id": "545373623514", "item_name": "同位竞品2", "qty_increase_ratio": "30", "date_range": "2017-01-02"},
    {"item_id": "544843732679", "item_name": "同位竞品3", "qty_increase_ratio": "30", "date_range": "2017-01-02"},
    {"item_id": "537361444972", "item_name": "ROEM罗燕 依恋旗下衣女", "qty_increase_ratio": "30", "date_range": "2017-01-03"},
    {"item_id": "538919327302", "item_name": "同位竞品1", "qty_increase_ratio": "30", "date_range": "2017-01-03"},
    {"item_id": "545373623514", "item_name": "同位竞品2", "qty_increase_ratio": "30", "date_range": "2017-01-03"},
    {"item_id": "544843732679", "item_name": "同位竞品3", "qty_increase_ratio": "30", "date_range": "2017-01-03"},
]
