# coding: utf-8
from __future__ import unicode_literals

import sys
reload(sys)
sys.setdefaultencoding("utf-8")
import traceback
import numpy as np

'''
main function
'''

ItemID, DateRange, itemattrdesc, attrname  = list(np.random.choice([""], size=4))


'''
main function
'''

def export_data(attrvalue, error_info):
    '''
    export data to hive client
    :param spu_price:
    :param error_info:
    :return:
    '''
    print "\t".join([unicode(ItemID), unicode(DateRange), unicode(itemattrdesc), unicode(attrname), unicode(attrvalue), unicode(error_info)])


def get_attrvalue(itemattrdesc, attrname):
    attrvalue = ""
    if itemattrdesc:
        data = itemattrdesc.split(" && ")
        for attr_st in data:
            attr = attr_st.split(": ")
            if len(attr) == 2 and attr[0] == attrname:
                attrvalue = attr[1]
                break
    return attrvalue


for line in sys.stdin:
    try:
        ItemID, DateRange, itemattrdesc, attrname = [_.replace("\\N", "") for _ in line.strip().split('\t')]

        attrvalue = get_attrvalue(itemattrdesc, attrname)
        export_data(attrvalue, "")

    except Exception, e:
        erro_info = traceback.format_exc().decode().replace("\t", "  ").replace("\n", "  ;;;;")
        export_data("", erro_info)

