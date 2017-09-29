# coding: utf-8
# __author__: ""
####################################### 店铺运营 -> 单价优化 #######################################
# 请求(默认)参数
{
    "interval": [
        {"name": "start_date", "value": "2017-01-02"},
        {"name": "end_date", "value": "2017-05-02"},
        {"name": "date_type", "value": "1"}
    ],
    "category": [
        {"name": "连衣裙", "value": "13232", "select": True},
        {"name": "衬衫", "value": "34231", "select": False},
        {"name": "短裤", "value": "6437", "select": False},
        {"name": "长袖", "value": "34352", "select": False},
    ],

}


### 寻找最优单价
# 参数
{
    "start_date": "2017-01-02",
    "end_date": "2017-05-02",
    "date_type": "1",
    "category": "1622",
    "start_price": "20",
    "end_price": "200",
    "search": "538919327302"
}

# 返回内容
{
    "price": [
        {"rank": "1", "item_id": "538919327302", "item_name": "ROEM罗燕 依恋旗下---34903（吊牌698）深蓝色/杏色甜美线毛衣女", "item_pic": "", "item_url": "https://item.taobao.com/item.htm?id=538919327302", "shop_name": "韩都衣舍旗舰店", "shop_id": "58501945", "price": "321", "qty": "234", "suggest_price_drift": "30", "suggest_price_drift_qty_ratio": "30", "suggest_event_price": "50", "suggest_event_price_qty_ratio": "30"},
        {"rank": "2", "item_id": "532844922420", "item_name": "2016秋冬新款棉衣女长袖短款内胆大码加厚修身中老年轻薄保暖内衣", "item_pic": "", "item_url": "https://item.taobao.com/item.htm?id=538919327302", "shop_name": "韩都衣舍旗舰店", "shop_id": "103314362", "price": "321", "qty": "234", "suggest_price_drift": "-30", "suggest_price_drift_qty_ratio": "30", "suggest_event_price": "10", "suggest_event_price_qty_ratio": "30"},
        {"rank": "3", "item_id": "538919327302", "item_name": "ROEM罗燕 依恋旗下---34903（吊牌698）深蓝色/杏色甜美线毛衣女", "item_pic": "", "item_url": "https://item.taobao.com/item.htm?id=538919327302", "shop_name": "韩都衣舍旗舰店", "shop_id": "58501945", "price": "321", "qty": "234", "suggest_price_drift": "-20", "suggest_price_drift_qty_ratio": "30", "suggest_event_price": "100", "suggest_event_price_qty_ratio": "30"},
        {"rank": "4", "item_id": "537361444972", "item_name": "韩版百搭钉珠镶钻领冰丝针织无袖上衣修身显瘦吊带背心女打底衫夏", "item_pic": "", "item_url": "https://item.taobao.com/item.htm?id=538919327302", "shop_name": "韩都衣舍旗舰店", "shop_id": "112067908", "price": "321", "qty": "234", "suggest_price_drift": "30", "suggest_price_drift_qty_ratio": "30", "suggest_event_price": "49", "suggest_event_price_qty_ratio": "30"},
        {"rank": "5", "item_id": "545373623514", "item_name": "2016春装新款气质小香风性感修身名媛水钻钉珠连衣裙acp", "item_pic": "", "item_url": "https://item.taobao.com/item.htm?id=538919327302", "shop_name": "韩都衣舍旗舰店", "shop_id": "59239838", "price": "321", "qty": "234", "suggest_price_drift": "30", "suggest_price_drift_qty_ratio": "30", "suggest_event_price": "70", "suggest_event_price_qty_ratio": "30"},
        {"rank": "6", "item_id": "544843732679", "item_name": "玮玛2017春装新款女装复古文艺淑女娃娃领纯棉修身印花连衣裙182H", "item_pic": "", "item_url": "https://item.taobao.com/item.htm?id=538919327302", "shop_name": "韩都衣舍旗舰店", "shop_id": "59084964", "price": "321", "qty": "234", "suggest_price_drift": "-10", "suggest_price_drift_qty_ratio": "30", "suggest_event_price": "83", "suggest_event_price_qty_ratio": "30"}
    ],
    "price_conclusion": {
        "item_name": "Lily2017春xxxxxx", 
        "drift": -50,
    }
}