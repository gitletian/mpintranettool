# coding: utf-8
# __author__: ""
from __future__ import print_function, unicode_literals, division

from six import iteritems

for k, v in iteritems({"ddd": "vvvv", "aaaa": "aaaa"}):
    print(k)
    print(v)


for k, v in {"ddd": "vvvv", "aaaa": "aaaa"}.items():
    print(k)
    print(v)


for k, v in {"ddd": "vvvv", "aaaa": "aaaa"}.iteritems():
    print(k)
    print(v)


