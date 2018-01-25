# coding: utf-8
from __future__ import unicode_literals

import sys
reload(sys)
sys.setdefaultencoding("utf-8")

import jieba

for line in sys.stdin:
    print "|".join(jieba.cut(line.strip()))
