# coding: utf-8
# __author__: ""
from __future__ import unicode_literals

import re
import pdb


def test1():

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

'^(?!.*hello)'
# m = re.match('(?!以前|之前|以往|已往|曾经|过去|当时|那时).{0,5}合生元', '曾dd经hod合生元')

m = re.match('(?=之前)合生元', '之前合生元')


print m

