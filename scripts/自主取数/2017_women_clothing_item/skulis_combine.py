# coding: utf-8
from __future__ import unicode_literals

import sys
reload(sys)
sys.setdefaultencoding("utf-8")
import json
import traceback


dateRange, platform, salesQty, salesAmt, stock, skulist, skudesc, ItemID = [""] * 8
platform_map = dict(tmall=7011, taobao=7001)


def export_data(export_skulist, platformid, error_info):
    '''
    export data to hive client
    :param export_skulist: 处理后的skulist
    :param error_info:
    :return:
    '''
    print "\t".join([unicode(dateRange), unicode(platformid), unicode(salesQty), unicode(salesAmt), unicode(stock), unicode(export_skulist), unicode(ItemID), unicode(error_info)])


def parser_skulist(skulist, skudesc):
    if not skulist:
        return ''

    sku_info = {x[0]: {"skuid": x[0], "stock": x[3], "discountprice": x[2], "listprice": x[1]} for x in [_.split(";") for _ in skulist.split(":")] if len(x) == 4}

    if skudesc and "^" in skudesc and "#" in skudesc:
        try:
            info = skudesc.split("^")

            desc_map = {x[0]: ":".join(x[1:]) for x in [_.split(":") for _ in "^".join(info[:-1]).split("@")] if len(x) > 1}
            desc_info = {x[1]: x[0] for x in [_.split("#") for _ in info[-1].split("@")] if len(x) > 1}

            for key, value in desc_info.iteritems():
                desc_list = [v.split(":") for v in value.split(";")]
                desc_data = [":".join([desc_map[r1] for r1 in r]) for r in desc_list if desc_map.has_key(r[0]) and desc_map.has_key(r[1])]

                if sku_info.has_key(key):
                    sku_info.get(key).update(skuname=";".join(desc_data), pvs=value)
        except Exception, e:
            pass

    return json.dumps(sku_info.values(), ensure_ascii=False)


if __name__ == '__main__':
    for line in sys.stdin:
        try:
            dateRange, platform, salesQty, salesAmt, stock, skulist, skudesc, ItemID = [_.replace("\\N", "") for _ in line.strip().split('\t')]

            export_skulist = parser_skulist(skulist, skudesc)
            platformid = platform_map.get(platform)

            export_data(export_skulist, platformid, '')

        except Exception, e:
            erro_info = traceback.format_exc().decode().replace("\t", "  ").replace("\n", "  ;;;;")
            export_data("",  "", erro_info)






