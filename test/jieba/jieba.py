# coding: utf-8
from __future__ import unicode_literals

import sys
reload(sys)
sys.setdefaultencoding("utf-8")

import jieba
#导入自定义词典
jieba.load_userdict("dict.txt")

for line in sys.stdin:
    id, content = line.strip().split('\t')
    new_content = "|".join(jieba.cut(content))
    print "\t".join([id, new_content])
