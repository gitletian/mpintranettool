# coding: utf-8
# __author__: ""
from __future__ import unicode_literals

import re
import pdb
import time

begin = time.time()
for i in range(100000000):
    content = "I am noob from runoob.com" + str(i)  # 59.9992961884
    # isMatch = re.search("runoob", content) # 192.789798975

print time.time() - begin