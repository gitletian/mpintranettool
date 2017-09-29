# coding: utf-8
from __future__ import unicode_literals

import sys
reload(sys)
sys.setdefaultencoding("utf-8")

from scipy import spatial
import traceback
import math


data_array = ["" for n in range(19)]
return_data = ["" for n in range(2)]


def export_data(return_data, error_info):
    '''

    :param return_data:
    :param error_info:
    :return:
    '''
    return_data = [unicode(data) for data in return_data]
    return_data.append(unicode(error_info))
    print "\t".join(return_data)


def process_num(s_data, t_data):
    t_data = float(t_data)
    s_data = float(s_data)
    return t_data / s_data if s_data > t_data else s_data / t_data


def process_cosine(s_data, t_data):
    result = 1 - spatial.distance.cosine([float(data) for data in s_data], [float(data) for data in t_data])
    return 0 if math.isnan(result) else result


def salesamt_zhanbi(s_data, t_data):

    if float(t_data) == 0:
        return 0

    return float(s_data) / float(t_data)


def process_data(data_array):
    s_shop_spu = process_num(data_array[1], data_array[10])
    s_shop_salesqty = process_num(data_array[2], data_array[11])
    s_shop_salesamt = process_num(data_array[3], data_array[12])
    s_shop_avg_price = process_num(data_array[4], data_array[13])

    s_category_spu = process_cosine(data_array[5].split(','), data_array[14].split(','))
    s_category_salesqty = process_cosine(data_array[6].split(','), data_array[15].split(','))
    s_category_salesamt = process_cosine(data_array[7].split(','), data_array[16].split(','))

    s_style_spu = process_cosine(data_array[9].split(','), data_array[18].split(','))

    amt_zhanbi = salesamt_zhanbi(data_array[3], data_array[12])
    if s_category_spu < 0.5 or 0.2 > amt_zhanbi or amt_zhanbi > 5.0:
        return [data_array[0], 0]

    a_data = [s_shop_spu, s_shop_salesqty, s_shop_avg_price, s_category_salesqty, s_category_salesamt]

    finalsim = process_cosine(a_data, [1.0] * 5)

    finalsim = process_cosine([finalsim, s_style_spu, s_shop_salesamt], [1.0] * 3)

    return [data_array[0], finalsim]


if __name__ == '__main__':
    for line in sys.stdin:
        try:
            data_array = [_.replace("\\N", "") for _ in line.strip().split('\t')]

            return_data = process_data(data_array)
            export_data(return_data, '')

        except Exception, e:
            erro_info = traceback.format_exc().decode().replace("\t", "  ").replace("\n", "  ;;;;")
            export_data(return_data,  erro_info)
