# coding: utf-8
# __author__: ""
####################################### 商品企划 -> 风尚元素 #######################################
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
    "method": [
        {"name": "环比增长", "value": "1", "select": True},
        {"name": "同比增长", "value": "2", "select": False}
    ],
    "interval": [
        {"name": "start_date", "value": "2017-01-02"},
        {"name": "end_date", "value": "2017-05-02"},
        {"name": "date_type", "value": "1"}
    ],
    "style": [
        {"name": "OL", "value": "OL", "select": True},
        {"name": "淑女", "value": "淑女", "select": False},
        {"name": "韩版", "value": "韩版", "select": False},
        {"name": "甜美", "value": "甜美", "select": False}
    ],
    "base_element": [
        {"name": "颜色", "value": "颜色", "select": True},
        {"name": "图案", "value": "图案", "select": True},
        {"name": "版型", "value": "版型", "select": True},
        {"name": "材质", "value": "材质", "select": False}
    ],
    "plus_element": [
        {"name": "裙长", "value": "裙长", "select": True},
        {"name": "腰型", "value": "腰型", "select": False},
        {"name": "轮廓", "value": "轮廓", "select": False},
        {"name": "工艺", "value": "工艺", "select": False},
    ],
    "kpi": [
        {"name": "增长最快", "value": "1", "select": False},
        {"name": "销售额最多", "value": "2", "select": True},
        {"name": "综合最大", "value": "3", "select": False},
    ],

}


#### 请求参数-总
{
    "platform": "7001",
    "market": "1",
    "follow_shop": "59084964,103314362,58501945",
    "method": "1",
    "start_date": "2017-01-02",
    "end_date": "2017-05-02",
    "date_type": "2"
}

### 畅销风尚洞悉
## 畅销风尚洞悉-举荐畅销
# 风格之最抢手
{
    "latest": [
        {"name": "泼西米亚风", "sales": "29576", "share": "56", "sales_increase_ratio": "151", "category_cover": "13", "category_top_3": "连衣裙,婚纱,衬衫", "is_follow": False},
        {"name": "民族风", "sales": "32343", "share": "32", "sales_increase_ratio": "12", "category_cover": "12", "category_top_3": "袜子,T恤,风衣", "is_follow": False},
        {"name": "街头", "sales": "5434", "share": "65", "sales_increase_ratio": "54", "category_cover": "10", "category_top_3": "袜子,短袖,毛衣", "is_follow": False}
    ],
    "latest_conclusion": {}, 
    "periods": [
        {"name": "泼西米亚风", "sales_increase_ratio": "34", "date_range": "2017-01"},
        {"name": "民族风", "sales_increase_ratio": "43", "date_range": "2017-01"},
        {"name": "街头", "sales_increase_ratio": "122", "date_range": "2017-01"},
        {"name": "泼西米亚风", "sales_increase_ratio": "34", "date_range": "2017-02"},
        {"name": "民族风", "sales_increase_ratio": "43", "date_range": "2017-02"},
        {"name": "街头", "sales_increase_ratio": "122", "date_range": "2017-02"}
    ],
    "periods_conclusion": {}
}

# 基础元素最强组合
{
    "latest": [
        {"name": "OL+黑色+豹纹+A字版+裙长", "sales": "29576", "share": "56", "sales_increase_ratio": "151", "category_cover": "13", "category_top": "连衣裙,婚纱,衬衫", "is_follow": False},
        {"name": "OL+绿色+豹纹+A字版+超短", "sales": "32343", "share": "32", "sales_increase_ratio": "12", "category_cover": "12", "category_top": "袜子,T恤,风衣", "is_follow": False},
        {"name": "OL+白色+豹纹+铅笔+裙长", "sales": "5434", "share": "65", "sales_increase_ratio": "54", "category_cover": "10", "category_top": "袜子,短袖,毛衣", "is_follow": False}
    ],
    "latest_conclusion": {}, 
    "periods": [
        {"name": "OL+黑色+豹纹+A字版+裙长", "sales_increase_ratio": "34", "date_range": "2017-01"},
        {"name": "OL+绿色+豹纹+A字版+超短", "sales_increase_ratio": "43", "date_range": "2017-01"},
        {"name": "OL+白色+豹纹+铅笔+裙长", "sales_increase_ratio": "122", "date_range": "2017-01"},
        {"name": "OL+黑色+豹纹+A字版+裙长", "sales_increase_ratio": "34", "date_range": "2017-02"},
        {"name": "OL+绿色+豹纹+A字版+超短", "sales_increase_ratio": "43", "date_range": "2017-02"},
        {"name": "OL+白色+豹纹+铅笔+裙长", "sales_increase_ratio": "122", "date_range": "2017-02"}
    ],
    "periods_conclusion": {}
}



## 畅销风尚洞悉-发现畅销
# 请求参数-附加
{
    "category": "1622",
    "style": "OL,淑女,甜美",
    "base_element": "图案,面料",
    "plus_element": "裙长, 工艺"
}
# 返回内容
{
    "latest": [
        {"name": "OL+黑色+豹纹+A字版+裙长", "sales": "29576", "share": "56", "sales_increase_ratio": "151", "category_cover": "13", "is_follow": False},
        {"name": "OL+绿色+豹纹+A字版+超短", "sales": "32343", "share": "32", "sales_increase_ratio": "12", "category_cover": "12", "is_follow": False},
        {"name": "OL+白色+豹纹+铅笔+裙长", "sales": "5434", "share": "65", "sales_increase_ratio": "54", "category_cover": "10", "is_follow": False}
    ],
    "latest_conclusion": {}, 
    "periods": [
        {"name": "OL+黑色+豹纹+A字版+裙长", "sales_increase_ratio": "34", "date_range": "2017-01"},
        {"name": "OL+绿色+豹纹+A字版+超短", "sales_increase_ratio": "43", "date_range": "2017-01"},
        {"name": "OL+白色+豹纹+铅笔+裙长", "sales_increase_ratio": "122", "date_range": "2017-01"},
        {"name": "OL+黑色+豹纹+A字版+裙长", "sales_increase_ratio": "34", "date_range": "2017-02"},
        {"name": "OL+绿色+豹纹+A字版+超短", "sales_increase_ratio": "43", "date_range": "2017-02"},
        {"name": "OL+白色+豹纹+铅笔+裙长", "sales_increase_ratio": "122", "date_range": "2017-02"}
    ],
    "periods_conclusion": {}
}


### 新风尚洞悉
## 新风尚洞悉-举荐畅销
# 风格之最新鲜
{
    "latest": [
        {"name": "泼西米亚风", "sales": "29576", "share": "56", "sales_increase_ratio": "151", "category_cover": "13", "category_top": "连衣裙,婚纱,衬衫", "is_follow": False},
        {"name": "民族风", "sales": "32343", "share": "32", "sales_increase_ratio": "12", "category_cover": "12", "category_top": "袜子,T恤,风衣", "is_follow": False},
        {"name": "街头", "sales": "5434", "share": "65", "sales_increase_ratio": "54", "category_cover": "10", "category_top": "袜子,短袖,毛衣", "is_follow": False}
    ],
    "latest_conclusion": {}, 
    "periods": [
        {"name": "泼西米亚风", "sales_increase_ratio": "34", "date_range": "2017-01"},
        {"name": "民族风", "sales_increase_ratio": "43", "date_range": "2017-01"},
        {"name": "街头", "sales_increase_ratio": "122", "date_range": "2017-01"},
        {"name": "泼西米亚风", "sales_increase_ratio": "34", "date_range": "2017-02"},
        {"name": "民族风", "sales_increase_ratio": "43", "date_range": "2017-02"},
        {"name": "街头", "sales_increase_ratio": "122", "date_range": "2017-02"}
    ],
    "periods_conclusion": {}
}


# 打底元素最新组合
{
    "latest": [
        {"name": "OL+黑色+豹纹+A字版+裙长", "sales": "29576", "share": "56", "sales_increase_ratio": "151", "category_cover": "13", "category_top": "连衣裙,婚纱,衬衫", "is_follow": False},
        {"name": "OL+绿色+豹纹+A字版+超短", "sales": "32343", "share": "32", "sales_increase_ratio": "12", "category_cover": "12", "category_top": "袜子,T恤,风衣", "is_follow": False},
        {"name": "OL+白色+豹纹+铅笔+裙长", "sales": "5434", "share": "65", "sales_increase_ratio": "54", "category_cover": "10", "category_top": "袜子,短袖,毛衣", "is_follow": False}
    ],
    "latest_conclusion": {}, 
    "periods": [
        {"name": "OL+黑色+豹纹+A字版+裙长", "sales_increase_ratio": "34", "date_range": "2017-01"},
        {"name": "OL+绿色+豹纹+A字版+超短", "sales_increase_ratio": "43", "date_range": "2017-01"},
        {"name": "OL+白色+豹纹+铅笔+裙长", "sales_increase_ratio": "122", "date_range": "2017-01"},
        {"name": "OL+黑色+豹纹+A字版+裙长", "sales_increase_ratio": "34", "date_range": "2017-02"},
        {"name": "OL+绿色+豹纹+A字版+超短", "sales_increase_ratio": "43", "date_range": "2017-02"},
        {"name": "OL+白色+豹纹+铅笔+裙长", "sales_increase_ratio": "122", "date_range": "2017-02"}
    ],
    "periods_conclusion": {}
}


## 畅销风尚洞悉-发现畅销
# 请求参数-附加
{
    "category": "1622",
    "style": "OL,淑女,甜美",
    "base_element": "图案,面料",
    "plus_element": "裙长, 工艺",
    "kpi": "1"
}
# 返回内容
{
    "latest": [
        {"name": "OL+黑色+豹纹+A字版+裙长", "sales": "29576", "share": "56", "sales_increase_ratio": "151", "category_cover": "13", "is_follow": False},
        {"name": "OL+绿色+豹纹+A字版+超短", "sales": "32343", "share": "32", "sales_increase_ratio": "12", "category_cover": "12", "is_follow": False},
        {"name": "OL+白色+豹纹+铅笔+裙长", "sales": "5434", "share": "65", "sales_increase_ratio": "54", "category_cover": "10", "is_follow": False}
    ],
    "latest_conclusion": {}, 
    "periods": [
        {"name": "OL+黑色+豹纹+A字版+裙长", "sales_increase_ratio": "34", "date_range": "2017-01"},
        {"name": "OL+绿色+豹纹+A字版+超短", "sales_increase_ratio": "43", "date_range": "2017-01"},
        {"name": "OL+白色+豹纹+铅笔+裙长", "sales_increase_ratio": "122", "date_range": "2017-01"},
        {"name": "OL+黑色+豹纹+A字版+裙长", "sales_increase_ratio": "34", "date_range": "2017-02"},
        {"name": "OL+绿色+豹纹+A字版+超短", "sales_increase_ratio": "43", "date_range": "2017-02"},
        {"name": "OL+白色+豹纹+铅笔+裙长", "sales_increase_ratio": "122", "date_range": "2017-02"}
    ],
    "periods_conclusion": {}
}


