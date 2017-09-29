# coding: utf-8
from __future__ import unicode_literals

import sys
reload(sys)
sys.setdefaultencoding("utf-8")
import traceback


def read_file(file_name):
    with open(file_name, "rb") as f:
        data = f.read()
        return data

if __name__ == '__main__':

    for line in sys.stdin:
        try:

            season, crawldate= line.strip().split('\t')

            crawldate = read_file("nohupsql.sh")
            print "\t".join([season, crawldate, ''])

        except Exception, e:
            erro_info = traceback.format_exc().decode().replace("\t", "  ").replace("\n", "  ;;;;")
            print "\t".join(["", "",  erro_info])






'''
## 用法
add file /home/script/tmp/read_file_udf_2.py;

add file /home/script/tmp/nohupsql.sh;

SELECT
  TRANSFORM (categoryid,categoryname)
  USING 'python read_file_udf_2.py'
  AS (categoryid,categoryname)
FROM elengjing.category limit 4;



'''

