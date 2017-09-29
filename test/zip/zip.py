# coding: utf-8
# __author__: ""


# z = zipfile.ZipFile('/Users/guoyuanpei/Documents/tmp_data/shanshan/item.zip', 'w', zipfile. ZIP_STORED, allowZip64=True)
# z.write('/Users/guoyuanpei/Documents/tmp_data/shanshan/item.csv', '/Users/guoyuanpei/Documents/tmp_data/shanshan/item.zip', zipfile.ZIP_DEFLATED)



import bz2
from datetime import datetime

print datetime.now()
output = bz2.BZ2File('/Users/guoyuanpei/Documents/tmp_data/shanshan/test/comment_lsk.bz2', 'wb')
try:
    with open("/Users/guoyuanpei/Documents/tmp_data/shanshan/test/comment_lsk.csv", 'rb') as f:
        # for line in f.readlines():
        #     output.write(line)

        while True:
            chunk = f.read(20480)
            if not chunk:
                break
            output.write(chunk)
finally:
    output.close()

print datetime.now()

import zipfile
z = zipfile.ZipFile('/Users/guoyuanpei/Documents/tmp_data/shanshan/test/comment_lsk.zip', 'w', zipfile. ZIP_STORED, allowZip64=True)
z.write('/Users/guoyuanpei/Documents/tmp_data/shanshan/test/comment_lsk.csv', '/Users/guoyuanpei/Documents/tmp_data/shanshan/test/comment_lsk.zip', zipfile.ZIP_DEFLATED)
print datetime.now()


#
# 2017-07-28 17:11:39.420663
# 2017-07-28 17:12:35.576283
# 2017-07-28 17:12:42.001628