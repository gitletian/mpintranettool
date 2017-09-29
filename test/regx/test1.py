# coding: utf-8
# __author__: ""
from __future__ import unicode_literals

import re
import pdb

patt = re.compile(r'(?<=<img src=")[^"]*', re.I)
str = '<div><img src="http://www.baidu.com/img/a.jpg"/><img src="http://www.badu.com/ll/a.jpg"/></div>'
pdb.set_trace()
re.match(patt, str)
r = patt.findall(str)







patt = re.compile(r'(?<=name=).+', re.I)

patt = re.compile(r'(?<!name=).+')

r = patt.findall('name=Zjmainstay')


re.match(r'(.*(?<!奶粉.{0})合生元)', '奶粉ddd合生元').group()
re.match(r'(.*(?<!奶粉.{1})合生元)', '奶粉ddd合生元').group()
re.match(r'(.*(?<!奶粉.{2})合生元)', '奶粉ddd合生元').group()