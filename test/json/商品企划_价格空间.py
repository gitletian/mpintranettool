# coding: utf-8
# __author__: ""

####################################### 商品企划 -> 价格空间 #######################################
# 请求(默认)参数
{
    "platform": [
        {"name": "淘宝", "value": "7001", "select": True},
        {"name": "天猫", "value": "7002", "select": False}
    ],
    "market": [
        {"name": "关注的店铺", "value": "1", "select": True},
        {"name": "同位市场", "value": "2", "select": False},
        {"name": "同价市场", "value": "3", "select": False},
        {"name": "全网市场", "value": "4", "select": False},
    ],
    "category": [
        {"name": "连衣裙", "value": "13232", "select": True},
        {"name": "衬衫", "value": "34231", "select": False},
        {"name": "短裤", "value": "6437", "select": False},
        {"name": "长袖", "value": "34352", "select": False},
    ],
    "interval": [
        {"name": "start_date", "value": "2017-01-02"},
        {"name": "end_date", "value": "2017-05-02"},
        {"name": "date_type", "value": "1"}
    ],
    "follow_hot": [
        {"name": "泼西米亚风", "value": "1", "select": True},
        {"name": "民族风", "value": "2", "select": False},
        {"name": "街头", "value": "3", "select": False},
        {"name": "泼西米亚风", "value": "4", "select": False},
        {"name": "OL+黑色+豹纹+A字版+裙长", "value": "34", "select": False},
        {"name": "OL+绿色+豹纹+A字版+超短", "value": "43", "select": False},
        {"name": "OL+白色+豹纹+铅笔+裙长", "value": "122", "select": False},
    ],
    "follow_new": [
        {"name": "泼西米亚风", "value": "1", "select": True},
        {"name": "民族风", "value": "2", "select": False},
        {"name": "街头", "value": "3", "select": False},
        {"name": "泼西米亚风", "value": "4", "select": False},
        {"name": "OL+黑色+豹纹+A字版+裙长", "value": "34", "select": True},
        {"name": "OL+绿色+豹纹+A字版+超短", "value": "43", "select": False},
        {"name": "OL+白色+豹纹+铅笔+裙长", "value": "122", "select": False},
    ],
}


#### 请求参数-总
{
    "platform": "7001",
    "market": "1",
    "follow_shop": "59084964,103314362,58501945",
    "category": "1622",
    "start_date": "2017-01-02",
    "end_date": "2017-05-02",
    "date_type": "2"
}

### 畅销风尚价格带
# 请求参数-附加
{
    "start_price": "200",
    "end_price": "300",
    "segment": "10",
    "follow_hot": "泼西米亚风,民族风,泼西米亚风,OL+黑色+豹纹+A字版+裙长"
}
# 返回内容
{
    "price": [
        {"name": "泼西米亚风", "sales": "4324222", "share": "56", "sales_ratio": "12", "market_sales_ratio": "14", "price": "(300,350]", "is_follow": False},
        {"name": "OL+黑色+豹纹+A字版+裙长", "sales": "5421", "share": "23", "sales_ratio": "9", "market_sales_ratio": "14", "price": "(300,350]", "is_follow": False},
        {"name": "民族风", "sales": "75234", "share": "3", "sales_ratio": "21", "market_sales_ratio": "11", "price": "(300,350]", "is_follow": False},
    ],
    "price_conclusion": {}
}

### 新风尚价格带
# 请求参数-附加
{
    "start_price": "200",
    "end_price": "30",
    "segment": "10",
    "follow_new": "泼西米亚风,民族风,泼西米亚风,OL+绿色+豹纹+A字版+超短"
}
# 返回内容
{
    "price": [
        {"name": "泼西米亚风", "sales": "4324222", "share": "56", "sales_ratio": "12", "market_sales_ratio": "14", "price": "(300,350]", "is_follow": False},
        {"name": "OL+黑色+豹纹+A字版+裙长", "sales": "5421", "share": "23", "sales_ratio": "9", "market_sales_ratio": "14", "price": "(300,350]", "is_follow": False},
        {"name": "民族风", "sales": "75234", "share": "3", "sales_ratio": "21", "market_sales_ratio": "11", "price": "(300,350]", "is_follow": False},
    ],
    "price_conclusion": {}
}



