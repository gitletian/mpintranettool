# coding: utf-8
# __author__: ""
from __future__ import unicode_literals
import pdb

import pickle

with open('/Users/guoyuanpei/Downloads/processed.pickle', 'rb') as inf:
    data1 = pickle.load(inf)
    output = ["\001".join([unicode(s) for s in _]) for _ in data1]

    with open('/Users/guoyuanpei/Downloads/processed.csv', 'wb') as outf:
        outf.write("\n".join(output))


print "end"







