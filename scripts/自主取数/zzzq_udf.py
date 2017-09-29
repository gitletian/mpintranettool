# coding: utf-8
from __future__ import unicode_literals

import sys
reload(sys)
sys.setdefaultencoding("utf-8")
import json
import traceback
import re

for line in sys.stdin:
    skustr1 = ""
    skustr2 = ""
    itemid = ""
    shopid = ""
    daterange = ""
    discountPrice = ""
    discountPrice2 = ""
    monthlySalesQty = ""
    skudisprice_today = ""
    skudisprice_yestady = ""
    platform = ""

    rereobj = re.compile(r"(\\[a-zA-Z0-9]{2,4})+")
    try:
        line = line.strip()
        arr2 = line.split('\t')
        arr = [_.replace("\\N", "") for _ in arr2]

        itemid, shopid, daterange, discountPrice, monthlySalesQty, jiaoyichenggong, skustr1, skustr2, skudisprice_today, skudisprice_yestady = (arr[0], arr[1], arr[2], arr[3], arr[4], arr[5], arr[6], arr[7], arr[8], arr[9])

        if not jiaoyichenggong or jiaoyichenggong == "":
            platform = "1"
        else:
            platform = "2"

        all_stock = 0
        all_qmt = 0
        sum_price = 0.0
        sum_count = 0

        if skudisprice_today and skudisprice_today.find("stock") != -1:

            if skudisprice_yestady == "":
                price_today = json.loads(skudisprice_today)["price"]
                for _ in price_today:
                    disprice = _.get("DiscountPrice")
                    if disprice != "":
                        sum_price += float(disprice)
                        sum_count += 1

                if sum_count != 0:
                    discountPrice2 = sum_price / float(sum_count)

                print "\t".join([itemid, shopid, daterange, unicode(discountPrice), unicode(discountPrice2), monthlySalesQty, skustr1, skustr2, unicode(sum_count), unicode(sum_price), platform, "0", skudisprice_today, skudisprice_yestady])

            elif skudisprice_today.find("sku") == -1 or skudisprice_yestady.find("sku") == -1:
                print "\t".join([itemid, shopid, daterange, unicode(discountPrice), unicode(discountPrice2), monthlySalesQty, skustr1, skustr2, "0", "0", platform, "b_1", skudisprice_today, skudisprice_yestady])

            else:
                price_today = json.loads(skudisprice_today)["price"]
                price_yestady = json.loads(skudisprice_yestady)["price"]

                sku_yestoday = {}
                for _ in price_yestady:
                    sku_yestoday.update({_["skuid"]: {"stock": _["stock"], "DiscountPrice": _["DiscountPrice"]}})
                flag = ""
                for _ in price_today:
                    if sku_yestoday.has_key(_["skuid"]) and _.has_key("DiscountPrice"):
                        stock = int(sku_yestoday.get(_["skuid"], {}).get("stock", _["stock"])) - int(_["stock"])

                        disprice = _.get("DiscountPrice")
                        if disprice != "":
                            all_stock += stock
                            all_qmt += (stock * float(disprice))

                            sum_price += float(disprice)
                            sum_count += 1

                if all_stock != 0:
                    discountPrice2 = all_qmt / float(all_stock)
                    print "\t".join(
                        [itemid, shopid, daterange, unicode(discountPrice), unicode(discountPrice2), monthlySalesQty,
                         skustr1,
                         skustr2, unicode(all_stock), unicode(all_qmt), platform, "", skudisprice_today,
                         skudisprice_yestady])
                elif sum_count != 0:
                    discountPrice2 = sum_price / float(sum_count)
                    print "\t".join(
                        [itemid, shopid, daterange, unicode(discountPrice), unicode(discountPrice2), monthlySalesQty,
                         skustr1,
                         skustr2, unicode(sum_count), unicode(sum_price), platform, "0", skudisprice_today,
                         skudisprice_yestady])
                else:
                    for _ in price_today:
                        sum_price += float(_["DiscountPrice"])
                    discountPrice2 = sum_price / float(len(price_today))
                    print "\t".join(
                        [itemid, shopid, daterange, unicode(discountPrice), unicode(discountPrice2), monthlySalesQty, skustr1,
                         skustr2, unicode(len(price_today)), unicode(sum_price), platform, "0", skudisprice_today, skudisprice_yestady])
            continue

        if not skustr1 or not skudisprice_today:
            print "\t".join(
                [itemid, shopid, daterange, unicode(discountPrice), unicode(discountPrice2), monthlySalesQty, skustr1, skustr2, "0", "0", platform,
                 "m_2", skudisprice_today, skudisprice_yestady])
            continue

        if skustr1.find("sku") == -1 or skudisprice_today.find("DiscountPrice") == -1:
            print "\t".join([itemid, shopid, daterange, unicode(discountPrice), unicode(discountPrice2), monthlySalesQty, skustr1, skustr2, "0", "0", platform, "m_1", skudisprice_today, skudisprice_yestady])
            continue

        skustr1 = re.sub(rereobj, " ", skustr1)
        skustr1 = skustr1.replace("'", "\"").replace("u\"", "\"")
        skulist1 = json.loads(skustr1)["skuMap"].values()
        sukdpList = json.loads(skudisprice_today)["price"]

        sku_dp_map = {}
        for _ in sukdpList:
            dp = _["DiscountPrice"]
            if dp == "":
                dp = _["ListPrice"]
            sku_dp_map.update({_["skuid"]: float(dp)})

        if not skustr2 or skustr2 != "":
            for _ in skulist1:
                if sku_dp_map.has_key(_["skuId"]):
                    disprice = sku_dp_map.get(_["skuId"])
                    if disprice != "":
                        sum_price += float(disprice)
                        sum_count += 1

            if sum_count != 0:
                discountPrice2 = sum_price / float(sum_count)
            print "\t".join([itemid, shopid, daterange, unicode(discountPrice), unicode(discountPrice2), monthlySalesQty, skustr1, skustr2, unicode(sum_count), unicode(sum_price), platform, "0", skudisprice_today, skudisprice_yestady])
            continue

        if skustr2.find("sku") == -1:
            print "\t".join([itemid, shopid, daterange, unicode(discountPrice), unicode(discountPrice2), monthlySalesQty, skustr1, skustr2, "0", "0", platform, "m_11", skudisprice_today, skudisprice_yestady])
            continue

        skustr2 = re.sub(rereobj, " ", skustr2)
        skustr2 = skustr2.replace("'", "\"").replace("u\"", "\"")
        skulist2 = json.loads(skustr2)["skuMap"].values()

        sku_yestoday = {}
        for _ in skulist2:
            sku_yestoday.update({_["skuId"]: {"stock": _["stock"], "price": _["price"]}})

        flag = ""
        for _ in skulist1:
            if sku_dp_map.has_key(_["skuId"]):

                stock = int(sku_yestoday.get(_["skuId"], {}).get("stock", _["stock"])) - int(_["stock"])

                disprice = sku_dp_map.get(_["skuId"])
                if disprice != "":
                    all_stock += stock
                    all_qmt += (stock * float(disprice))

                    sum_price += float(disprice)
                    sum_count += 1

        if all_stock != 0:
            discountPrice2 = all_qmt / float(all_stock)
            print "\t".join(
                [itemid, shopid, daterange, unicode(discountPrice), unicode(discountPrice2), monthlySalesQty, skustr1,
                 skustr2, unicode(all_stock), unicode(all_qmt), platform, flag, skudisprice_today, skudisprice_yestady])
        elif sum_count != 0:
            discountPrice2 = sum_price / float(sum_count)
            flag = "0"
            print "\t".join(
                [itemid, shopid, daterange, unicode(discountPrice), unicode(discountPrice2), monthlySalesQty, skustr1, skustr2,
                 unicode(sum_count), unicode(sum_price), platform, flag, skudisprice_today, skudisprice_yestady])

    except Exception, e:
        erro_info = traceback.format_exc().decode().replace("\t", "  ").replace("\n", "  ;;;;")
        print "\t".join([itemid, shopid, daterange, unicode(discountPrice), unicode(discountPrice2), monthlySalesQty, skustr1, skustr2, u"0", u"0", platform, erro_info, skudisprice_today, skudisprice_yestady])

