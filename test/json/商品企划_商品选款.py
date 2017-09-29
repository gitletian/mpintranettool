# coding: utf-8
# __author__: ""
####################################### 商品企划 -> 商品选款 #######################################
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
    "follow_price": [
        {"start_price": "400", "end_price": "450", "select": True},
        {"start_price": "450", "end_price": "500", "select": True},
        {"start_price": "500", "end_price": "550", "select": True},
        {"start_price": "550", "end_price": "600", "select": True},
        {"start_price": "600", "end_price": "650", "select": True},
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
    "date_type": "1",
    "follow_hot": "1,2,3",
    "follow_new": "1,2,3",
    "follow_price": "1,2,3"
}

###  商品选款
{
    "choice": [
        {"rank": 1, "item_id": "538919327302", "item_name": "ROEM罗燕 依恋旗下---34903（吊牌698）深蓝色/杏色甜美线毛衣女", "item_pic": "", "item_url": "https://item.taobao.com/item.htm?id=538919327302", "shop_name": "韩都衣舍旗舰店", "shop_id": "58501945", "listed_date": "2014-01-11", "price": "321", "favourite": "20", "qty": "234", "qty_link_increase_ratio": "30", "shop_qty_ratio": "10", "is_follow": False},
        {"rank": 2, "item_id": "532844922420", "item_name": "2016秋冬新款棉衣女长袖短款内胆大码加厚修身中老年轻薄保暖内衣", "item_pic": "", "item_url": "https://item.taobao.com/item.htm?id=538919327302", "shop_name": "韩都衣舍旗舰店", "shop_id": "103314362", "listed_date": "2014-01-11", "price": "321", "favourite": "20", "qty": "234", "qty_link_increase_ratio": "30", "shop_qty_ratio": "10", "is_follow": False},
        {"rank": 1, "item_id": "538919327302", "item_name": "ROEM罗燕 依恋旗下---34903（吊牌698）深蓝色/杏色甜美线毛衣女", "item_pic": "", "item_url": "https://item.taobao.com/item.htm?id=538919327302", "shop_name": "韩都衣舍旗舰店", "shop_id": "58501945", "listed_date": "2014-01-11", "price": "321", "favourite": "20", "qty": "234", "qty_link_increase_ratio": "30", "shop_qty_ratio": "10", "is_follow": False},
        {"rank": 3, "item_id": "537361444972", "item_name": "韩版百搭钉珠镶钻领冰丝针织无袖上衣修身显瘦吊带背心女打底衫夏", "item_pic": "", "item_url": "https://item.taobao.com/item.htm?id=538919327302", "shop_name": "韩都衣舍旗舰店", "shop_id": "112067908", "listed_date": "2014-01-11", "price": "321", "favourite": "20", "qty": "234", "qty_link_increase_ratio": "30", "shop_qty_ratio": "10", "is_follow": False},
        {"rank": 4, "item_id": "545373623514", "item_name": "2016春装新款气质小香风性感修身名媛水钻钉珠连衣裙acp", "item_pic": "", "item_url": "https://item.taobao.com/item.htm?id=538919327302", "shop_name": "韩都衣舍旗舰店", "shop_id": "59239838", "listed_date": "2014-01-11", "price": "321", "favourite": "20", "qty": "234", "qty_link_increase_ratio": "30", "shop_qty_ratio": "10", "is_follow": False},
        {"rank": 5, "item_id": "544843732679", "item_name": "玮玛2017春装新款女装复古文艺淑女娃娃领纯棉修身印花连衣裙182H", "item_pic": "", "item_url": "https://item.taobao.com/item.htm?id=538919327302", "shop_name": "韩都衣舍旗舰店", "shop_id": "59084964", "listed_date": "2014-01-11", "price": "321", "favourite": "20", "qty": "234", "qty_link_increase_ratio": "30", "shop_qty_ratio": "10", "is_follow": False}
    ],
    "choice_conclusion": {}
}

