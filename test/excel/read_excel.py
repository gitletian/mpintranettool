# coding: utf-8
# __author__: ""
from __future__ import print_function, unicode_literals, division




'''
The mode can be 'r', 'w' or 'a' for reading (default), writing or appending

'''
with open("/Users/guoyuanpei/workspace/pworkspace/mpintranettool/error_log1.txt", "ab") as f:
    for i in range(10):
        f.write("bbbbbbb")


