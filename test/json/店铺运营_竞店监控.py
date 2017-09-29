# coding: utf-8
# __author__: ""
####################################### 店铺运营 -> 竞店监控 #######################################
# 请求(默认)参数
{
    "interval": [
        {"name": "start_date", "value": "2017-01-02"},
        {"name": "end_date", "value": "2017-05-02"},
        {"name": "type", "value": "1"}
    ],
    "category": [
        {"name": "连衣裙", "value": "13232", "select": True},
        {"name": "衬衫", "value": "34231", "select": False},
        {"name": "短裤", "value": "6437", "select": False},
        {"name": "长袖", "value": "34352", "select": False},
    ],
    "follow_shop": [
        {"name": "韩都衣舍旗舰店", "value": "59084964", "select": True},
        {"name": "韩都衣舍旗舰店1", "value": "112067908", "select": False},
        {"name": "韩都衣舍旗舰店2", "value": "103314362", "select": False},
        {"name": "韩都衣舍旗舰店3", "value": "58501945", "select": False}
    ],
    "method": [
        {"name": "环比增长", "value": "1", "select": True},
        {"name": "同比增长", "value": "2", "select": False}
    ],
    "kpi": [
        {"name": "销售额", "value": "sales", "select": True},
        {"name": "spu数", "value": "spu", "select": False},
        {"name": "均单价", "valule": "avg_price", "select": False},
        {"name": "销量", "value": "qty", "select": False}
    ]
}


#### 请求参数-总
{
    "start_date": "2017-01-02",
    "end_date": "2017-05-02",
    "date_type": "1",
    "category": "1622",
    "follow_shop": "59084964,103314362,58501945"
}
### 上新及补货

{
    "new": [
        {"shop_name": "本店", "shop_id": "29576", "item_count": "56", "most_category": "连衣裙", "most_item_count": "34", "date_range": "06-09~07-01"},
        {"shop_name": "lili官方旗舰店", "shop_id": "29576", "new_item_count": "56", "most_category": "连衣裙", "most_item_count": "34", "date_range": "07-02~09-05"},
        {"shop_name": "only官方旗舰店", "shop_id": "29576", "new_item_count": "56", "most_category": "连衣裙", "most_item_count": "34", "date_range": "11-07~12-03"},
    ],
    "new_conclusion": {},
    "supply": [
        {"shop_name": "本店", "shop_id": "29576", "item_count": "56", "most_category": "连衣裙", "most_item_count": "34", "date_range": "06-09~07-01"},
        {"shop_name": "lili官方旗舰店", "shop_id": "29576", "item_count": "56", "most_category": "连衣裙", "most_item_count": "34", "date_range": "07-02~09-05"},
        {"shop_name": "only官方旗舰店", "shop_id": "29576", "item_count": "56", "most_category": "连衣裙", "most_item_count": "34", "date_range": "11-07~12-03"},
    ],
    "supply_conclusion": {}
}

### 店铺销售差异
## 参数
{
    "method": "1"
}

## 返回内容
{
    "latest": [
        {"shop_name": "本店", "shop_id": "29576", "sales": "1231", "link_increase_ratio": "20"},
        {"shop_name": "lili官方旗舰店", "shop_id": "29576", "sales": "1231", "link_increase_ratio": "20"},
        {"shop_name": "only官方旗舰店", "shop_id": "29576", "sales": "1231", "link_increase_ratio": "20"},
    ],
    "latest_conclusion": {},
    "period": [
        {"shop_name": "本店", "shop_id": "29576", "link_increase_ratio": "56", "date_range": "2017-01"},
        {"shop_name": "lili官方旗舰店", "shop_id": "29576", "link_increase_ratio": "56", "date_range": "2017-01"},
        {"shop_name": "only官方旗舰店", "shop_id": "29576", "link_increase_ratio": "56", "date_range": "2017-01"},
    ],
    "period_conclusion": {}
}



### 店铺品类销售额差异
# 访问参数:
{
    "kpi": "spu"
}

# 返回内容
[
    {"shop_name": "本店", "shop_id": "29576", "sales": "1231", "qty": "20", "spu": "30", "avg_price": "20", "category_name": "袜子", "category_id": "111"},
    {"shop_name": "lili官方旗舰店", "shop_id": "29576", "sales": "1231", "qty": "20", "spu": "30", "avg_price": "20", "category_name": "袜子", "category_id": "111"},
    {"shop_name": "only官方旗舰店", "shop_id": "29576", "sales": "1231", "qty": "20", "spu": "30", "avg_price": "20", "category_name": "袜子", "category_id": "111"},
]

### 店铺品类销售好坏
# 参数
{
    "start_price": "20",
    "end_price": "200",
    "search": "538919327302"
}

# 返回内容
{
    "quality": [
        {"item_id": "538919327302", "item_name": "ROEM罗燕 依恋旗下---34903（吊牌698）深蓝色/杏色甜美线毛衣女", "item_pic": "", "item_url": "https://item.taobao.com/item.htm?id=538919327302", "shop_name": "韩都衣舍旗舰店", "shop_id": "58501945", "listed_date": "2014-01-11", "price": "321", "favourite": "20", "qty": "234", "qty_link_increase_ratio": "30", "stock": "10", "is_follow": True},
        {"item_id": "532844922420", "item_name": "2016秋冬新款棉衣女长袖短款内胆大码加厚修身中老年轻薄保暖内衣", "item_pic": "", "item_url": "https://item.taobao.com/item.htm?id=538919327302", "shop_name": "韩都衣舍旗舰店", "shop_id": "103314362", "listed_date": "2014-01-11", "price": "321", "favourite": "20", "qty": "234", "qty_link_increase_ratio": "30", "stock": "10", "is_follow": False},
        {"item_id": "538919327302", "item_name": "ROEM罗燕 依恋旗下---34903（吊牌698）深蓝色/杏色甜美线毛衣女", "item_pic": "", "item_url": "https://item.taobao.com/item.htm?id=538919327302", "shop_name": "韩都衣舍旗舰店", "shop_id": "58501945", "listed_date": "2014-01-11", "price": "321", "favourite": "20", "qty": "234", "qty_link_increase_ratio": "30", "stock": "10", "is_follow": False},
        {"item_id": "537361444972", "item_name": "韩版百搭钉珠镶钻领冰丝针织无袖上衣修身显瘦吊带背心女打底衫夏", "item_pic": "", "item_url": "https://item.taobao.com/item.htm?id=538919327302", "shop_name": "韩都衣舍旗舰店", "shop_id": "112067908", "listed_date": "2014-01-11", "price": "321", "favourite": "20", "qty": "234", "qty_link_increase_ratio": "30", "stock": "10", "is_follow": False},
        {"item_id": "545373623514", "item_name": "2016春装新款气质小香风性感修身名媛水钻钉珠连衣裙acp", "item_pic": "", "item_url": "https://item.taobao.com/item.htm?id=538919327302", "shop_name": "韩都衣舍旗舰店", "shop_id": "59239838", "listed_date": "2014-01-11", "price": "321", "favourite": "20", "qty": "234", "qty_link_increase_ratio": "30", "stock": "10", "is_follow": False},
        {"item_id": "544843732679", "item_name": "玮玛2017春装新款女装复古文艺淑女娃娃领纯棉修身印花连衣裙182H", "item_pic": "", "item_url": "https://item.taobao.com/item.htm?id=538919327302", "shop_name": "韩都衣舍旗舰店", "shop_id": "59084964", "listed_date": "2014-01-11", "price": "321", "favourite": "20", "qty": "234", "qty_link_increase_ratio": "30", "stock": "10", "is_follow": True}
    ],
    "quality_conclusion": {},
    "total": 113
}

### 商品对比 加入对比-详情
# 参数
{
    "start_date": "2017-01-02",
    "end_date": "2017-05-02",
    "date_type": "1",
    "follow_item": "538919327302_58501945,544843732679_58501945,544843732679_58501945",
}

# 返回结果
[
    {"item_id": "538919327302", "item_name": "ROEM罗燕 依恋旗下---34903（吊牌698）深蓝色/杏色甜美线毛衣女", "shop_name": "韩都衣舍旗舰店", "shop_id": "58501945", "stock": "300", "favourite": "20", "price": "321", "qty": "234", "qty_link_increase_ratio": "10", "qty_year_increase_ratio": "10", "date_range": "2017-02-03"},
    {"item_id": "538919327302", "item_name": "ROEM罗燕 依恋旗下---34903（吊牌698）深蓝色/杏色甜美线毛衣女", "shop_name": "韩都衣舍旗舰店", "shop_id": "58501945", "stock": "300", "favourite": "20", "price": "321", "qty": "234", "qty_link_increase_ratio": "10", "qty_year_increase_ratio": "10", "date_range": "2017-02-03"},
    {"item_id": "538919327302", "item_name": "ROEM罗燕 依恋旗下---34903（吊牌698）深蓝色/杏色甜美线毛衣女", "shop_name": "韩都衣舍旗舰店", "shop_id": "58501945", "stock": "300", "favourite": "20", "price": "321", "qty": "234", "qty_link_increase_ratio": "10", "qty_year_increase_ratio": "10", "date_range": "2017-02-03"},
]

