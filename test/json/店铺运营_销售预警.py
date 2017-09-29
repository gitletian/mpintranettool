# coding: utf-8
# __author__: ""
####################################### 店铺运营 -> 销售预警 #######################################
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

}


### 脱销或积压预测
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
    "monitor": [
        {"rank": "1", "item_id": "538919327302", "item_name": "ROEM罗燕 依恋旗下---34903（吊牌698）深蓝色/杏色甜美线毛衣女", "item_pic": "", "item_url": "https://item.taobao.com/item.htm?id=538919327302", "shop_name": "韩都衣舍旗舰店", "shop_id": "58501945", "sales": "234", "stock": "300", "stock_consume_week": "1", "qty_last_two_weeks": "10", "qty": "245", "qty_link_increase_ratio": "10", "content": "备注", "is_follow": False},
        {"rank": "2", "item_id": "532844922420", "item_name": "2016秋冬新款棉衣女长袖短款内胆大码加厚修身中老年轻薄保暖内衣", "item_pic": "", "item_url": "https://item.taobao.com/item.htm?id=538919327302", "shop_name": "韩都衣舍旗舰店", "shop_id": "103314362", "sales": "234", "stock": "300", "stock_consume_week": "1", "qty_last_two_weeks": "-30", "qty": "245", "qty_link_increase_ratio": "10", "content": "备注", "is_follow": False},
        {"rank": "3", "item_id": "538919327302", "item_name": "ROEM罗燕 依恋旗下---34903（吊牌698）深蓝色/杏色甜美线毛衣女", "item_pic": "", "item_url": "https://item.taobao.com/item.htm?id=538919327302", "shop_name": "韩都衣舍旗舰店", "shop_id": "58501945", "sales": "234", "stock": "300", "stock_consume_week": "1", "qty_last_two_weeks": "-20", "qty": "245", "qty_link_increase_ratio": "10", "content": "备注", "is_follow": False},
        {"rank": "4", "item_id": "537361444972", "item_name": "韩版百搭钉珠镶钻领冰丝针织无袖上衣修身显瘦吊带背心女打底衫夏", "item_pic": "", "item_url": "https://item.taobao.com/item.htm?id=538919327302", "shop_name": "韩都衣舍旗舰店", "shop_id": "112067908", "sales": "234", "stock": "300", "stock_consume_week": "1", "qty_last_two_weeks": "30", "qty": "245", "qty_link_increase_ratio": "10", "content": "备注", "is_follow": False},
        {"rank": "5", "item_id": "545373623514", "item_name": "2016春装新款气质小香风性感修身名媛水钻钉珠连衣裙acp", "item_pic": "", "item_url": "https://item.taobao.com/item.htm?id=538919327302", "shop_name": "韩都衣舍旗舰店", "shop_id": "59239838", "sales": "234", "stock": "300", "stock_consume_week": "1", "qty_last_two_weeks": "30", "qty": "245", "qty_link_increase_ratio": "10", "content": "备注", "is_follow": False},
        {"rank": "6", "item_id": "544843732679", "item_name": "玮玛2017春装新款女装复古文艺淑女娃娃领纯棉修身印花连衣裙182H", "item_pic": "", "item_url": "https://item.taobao.com/item.htm?id=538919327302", "shop_name": "韩都衣舍旗舰店", "shop_id": "59084964", "sales": "234", "stock": "300", "stock_consume_week": "1", "qty_last_two_weeks": "-10", "qty": "245", "qty_link_increase_ratio": "10", "content": "备注", "is_follow": False}
    ],
    "monitor_conclusion": {}
}