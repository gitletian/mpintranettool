# coding: utf-8
# __author__: ""
from interval import Interval, IntervalSet


volume1 = Interval.between("A", "Foe")
volume2 = Interval.between("Fog", "McAfee")
volume3 = Interval.between("McDonalds", "Space")
volume4 = Interval.between("Spade", "Zygote")

encyclopedia = IntervalSet([volume1, volume2, volume3, volume4])

mySet = IntervalSet([volume1, volume3, volume4])

"Meteor" in encyclopedia
"Goose" in encyclopedia

"Goose" in mySet
volume2 in (encyclopedia ^ mySet)

print [(_.lower_bound, _.upper_bound) for _ in IntervalSet(encyclopedia)]


OfficeHours = IntervalSet.between("08:00", "17:00")
myLunch = IntervalSet.between("11:30", "12:30")
myHours = IntervalSet.between("08:30", "19:30") - myLunch
myHours.issubset(OfficeHours)

"12:00" in myHours

"15:30" in myHours

inOffice = OfficeHours & myHours

overtime = myHours - OfficeHours





r1 = IntervalSet([Interval(1, 1000), Interval(1100, 1200)])
r2 = IntervalSet([Interval(30, 50), Interval(60, 200), Interval(1150, 1300)])

r3 = IntervalSet([Interval(1000, 3000)])
r4 = IntervalSet([Interval(1000, 3000)])
r5 = IntervalSet([Interval(30000, 12000)])

print (r3 - r4), (r4 - r3), r3 & r4
print len(IntervalSet.empty())

if r3 & r4 == r4:
    print 'yes'

print r3 & r4
if (r3 - r4).empty():
   print "true"
print (r3 - r4).empty()



'''
interval对象初始化参数（lower_bound=-Inf, upper_bound=Inf, **kwargs）
三个boolean参数closed,lower_closed,upper_closed分表表示全闭，左闭右开，左开右闭。
    比如：r = Interval(upper_bound=62, closed=False) between(a, b, closed=True)：返回以a和b为界的区间

less_than(a)：小于a的所有值构成interval，类似的还有less_than_or_equal_to，greater_than，greater_than_or_equal_to函数

join(other)：将两个连续的intervals组合起来
overlaps(other)：两个区间是否有重叠
adjacent_to(other)：两个区间是否不重叠的毗邻

'''
