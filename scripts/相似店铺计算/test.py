# coding: utf-8
# __author__: ""

#encode=utf8
import pandas as pd

import pdb


ss1 = "通勤,甜美,街头,OL风格,民族风,宫廷,复古,英伦,文艺,乡村,韩版,简约,淑女,中性,嬉皮,运动休闲,摇滚,军装,海军,朋克,嘻哈,波普,欧美,森女,瑞丽,居家,波西米亚,洛丽塔,学院,田园,公主,日系"
ss2 = "背带,吊带,斜肩,挂脖式,裹胸,其他/other"
ss3 = "人物,纯色,花色,格子,圆点,条纹,手绘,卡通动漫,豹纹,千鸟格,字母,动物图案,其他,碎花,风景,抽象图案,建筑,斑马纹,大花,动物纹"
ss4 = "雪纺,欧根纱,织锦,羊皮,毛呢,牛仔布,绸缎,针织,猪皮,双绉,天鹅绒,府绸,开司米,法兰绒,轻薄花呢,牛皮,蕾丝,灯芯绒,其他"
ss5 = "乳白色,军绿色,卡其色,咖啡色,墨绿色,天蓝色,姜黄色,孔雀蓝,宝蓝色,巧克力色,明黄色,杏色,柠檬黄,栗色,桔红色,桔色,浅棕色,浅灰色,浅紫色,浅绿色,浅蓝色,浅黄色,深卡其布色,深棕色,深灰色,深紫色,深蓝色,湖蓝色,灰色,玫红色,白色,米白色,粉红色,紫红色,紫罗兰,紫色,红色,绿色,翠绿色,花色,荧光绿,荧光黄,蓝色,藏青色,藕色,褐色,西瓜红,透明,酒红色,金色,银色,青色,香槟色,驼色,黄色,黑色"

df1 = pd.DataFrame(ss1.split(","), columns=["value"])

df2 = pd.DataFrame(ss2.split(","), columns=["value"])
df3 = pd.DataFrame(ss3.split(","), columns=["value"])
df4 = pd.DataFrame(ss4.split(","), columns=["value"])
df5 = pd.DataFrame(ss5.split(","), columns=["value"])

df1["key"] = 1
df2["key"] = 1
df3["key"] = 1
df4["key"] = 1
df5["key"] = 1

dfa1 = pd.merge(df1, df2, on="key")
dfa2 = pd.merge(dfa1, df3, on="key")
dfa3 = pd.merge(dfa2, df4, on="key")
dfa4 = pd.merge(dfa3, df5, on="key")

del dfa4["key"]

dfa4.to_csv("style.csv", index=False, header=False, sep="\t")











# pd1.to_csv("style.csv", sep="\t", header=False, index=False)
