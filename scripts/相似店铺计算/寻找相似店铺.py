
# coding: utf-8
'''
1.店铺销售额-排名
2.（分品类│总）*（spu数│销售额│销售量）
3.平均spu单价
4.粉丝数/店铺等级                                       
5.店铺评分（三个评分）
6.店铺地理位置
7.开店时长
8.风格分布
9.年龄分布
10.材质分布
11.上市季节分布
12.自身店铺增长（销售额-3months
      经过讨论分为 像：重要的指标为：1，2，3，9，8   ，像：一般重要指标为：6，7，10，11  ，   不像：指标为4，5，12         
      最终决定  取重要的指标   ：   1，2，3，9，8 分为十个维度分析。标量：B/A  矢量：cos（α，β）                                                                                            
      另： 杨骏提醒软件部定价指导区分以下三种：
        1. 原价数据去训练模型决定要怎么定原价。
        2. 日常折扣价数据决定要怎么定日常的折扣价。
        3. 平台小活动数据决定怎么定平台小活动时的折扣价：平时节假日的活动。
        4. 平台大活动数据决怎么定平台大活动时的折扣价：双十一，双十二，年货节活动。
# In[ ]:

1. 各店铺销售额排名表： 店铺ID  行业排名
2. 各店铺SPU数、销售额、销量、平均件单价统计表： 店铺ID  SPU数  销售额  销量   平均件单价
3. 各店铺各品类SPU数：店铺ID  品类1  品类2 。。。  品类n
4. 各店铺各品类销售额：店铺ID  品类1  品类2 。。。  品类n
5. 各店铺各品类销售量：店铺ID  品类1  品类2 。。。  品类n
6. 各店铺各风格SPU数分布：店铺ID 风格1  风格2 。。。 风格n
7. 各店铺各适用年龄段SPU数分布：店铺ID 适用年龄段1  适用年龄段2 。。。 适用年龄段n
'''

# In[1]:

#encode=utf8
import pandas as pd
from scipy import spatial


# In[43]:

sid=73401272 #Lily 68985961 


# In[44]:

df1 = pd.read_csv('1shop.txt', header=None, names=['shopid', 'rank'], sep='\t')
num1=float(df1[df1['shopid']==sid]['rank'].values[0])
df1['rank']=df1['rank'].apply(lambda x: num1/x if x > num1 else x/num1)


df2 = pd.read_csv('2shop.txt', header=None, names=['shopid', 'shopname', 'spucount', 'salesamt', 'salescount', 'avgprice'], sep='\t')

num=float(df2[df2['shopid']==sid]['spucount'].values[0])
df2['spucount']=df2['spucount'].apply(lambda x: num/x if x > num else x/num)

num=float(df2[df2['shopid']==sid]['salesamt'].values[0])
df2['salesamt']=df2['salesamt'].apply(lambda x: num/x if x > num else x/num)

num=float(df2[df2['shopid']==sid]['salescount'].values[0])
df2['salescount']=df2['salescount'].apply(lambda x: num/x if x > num else x/num)

num=float(df2[df2['shopid']==sid]['avgprice'].values[0])
df2['avgprice']=df2['avgprice'].apply(lambda x: num/x if x > num else x/num)


df3 = pd.read_csv('3categorySPU_交叉表.txt',header=0,sep='\t').fillna(0)
del df3['总计 count']
nums = df3[df3['shopid']==sid]
del nums['shopid']
ls=[]

for i in range(0, df3.shape[0]):
    ls.append([int(df3.iloc[i][0]), 1 - spatial.distance.cosine(df3.iloc[i][1:], nums)])
df3 = pd.DataFrame(ls)
df3.columns=['shopid', 'spusim']

df4 = pd.read_csv('4category_amt_交叉表.txt', header=0, sep='\t').fillna(0)
del df4['总计 count']
nums = df4[df4['shopid']==sid]
del nums['shopid']
ls=[]

for i in range(0, df4.shape[0]):
    ls.append([int(df4.iloc[i][0]), 1-spatial.distance.cosine(df4.iloc[i][1:], nums)])
df4 = pd.DataFrame(ls)
df4.columns=['shopid', 'amtsim']


df5 = pd.read_csv('5category_salesqty_交叉表.txt', header=0, sep='\t').fillna(0)
del df5['总计 count']
nums = df5[df5['shopid']==sid]
del nums['shopid']
ls=[]

for i in range(0, df5.shape[0]):
    ls.append([int(df5.iloc[i][0]), 1-spatial.distance.cosine(df5.iloc[i][1:], nums)])
df5 = pd.DataFrame(ls)
df5.columns=['shopid', 'qtysim']


df6 = pd.read_csv('6shop_style_SPU_交叉表.txt', header=0, sep='\t').fillna(0)
del df6['总计']
nums = df6[df6['shopid']==sid]
del nums['shopid']
ls=[]

for i in range(0, df6.shape[0]):
    ls.append([int(df6.iloc[i][0]), 1-spatial.distance.cosine(df6.iloc[i][1:], nums)])
    
df6 = pd.DataFrame(ls)
df6.columns=['shopid', 'stylesim']


df7 = pd.read_csv('7shop_age_SPU_交叉表.txt', header=0, sep='\t').fillna(0)
del df7['总计 count']
nums = df7[df7['shopid']==sid]
del nums['shopid']
ls=[]

for i in range(0, df7.shape[0]):
    ls.append([int(df7.iloc[i][0]), 1-spatial.distance.cosine(df7.iloc[i][1:], nums)])
    
df7 = pd.DataFrame(ls)
df7.columns=['shopid', 'agesim']


# In[45]:

#result = pd.merge(df2,df1,on='shopid')
result = pd.merge(df2, df3, on='shopid')
result = pd.merge(result, df4, on='shopid')
result = pd.merge(result, df5, on='shopid')
result = pd.merge(result, df7, on='shopid')
#result = pd.merge(result,df6,on='shopid')
result.to_csv("SimMatrix.txt", sep='\t', index=False)

nums = result[result['shopid']==sid]
del nums['shopid']
del nums['shopname']
ls=[]


# In[46]:

for i in range(0,result.shape[0]):
    ls.append([int(result.iloc[i][0]), result.iloc[i][1], 1-spatial.distance.cosine(result.iloc[i][2:], nums)])
finalsim = pd.DataFrame(ls)
finalsim.columns=['shopid', 'shopname', 'sim']

finalsim = pd.merge(finalsim, df6, on='shopid')

nums = finalsim[finalsim['shopid']==sid]
del nums['shopid']
del nums['shopname']

ls2=[]
for i in range(0, finalsim.shape[0]):
    ls2.append([int(finalsim.iloc[i][0]), finalsim.iloc[i][1], 1-spatial.distance.cosine(finalsim.iloc[i][2:], nums)])
finalsim2 = pd.DataFrame(ls2)


# In[47]:

finalsim2.columns=['shopid', 'shopname', 'sim']
finalsim2=finalsim2.sort(['sim'], ascending=[0])
finalsim2.to_csv("SimShops-"+str(sid)+"-B.txt", sep='\t', index=False)



