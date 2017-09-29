# coding: utf-8
from __future__ import unicode_literals

import sys
reload(sys)
sys.setdefaultencoding("utf-8")

import traceback


data_array = [""] * 6


def export_data(untag, desc_count, untag_count, error_info):
    '''

    :param untag:
    :param error_info:
    :return:
    '''
    return_data = [unicode(data) for data in [data_array[0], data_array[5], desc_count, untag_count, untag, error_info]]
    print "\t".join(return_data)


if __name__ == '__main__':
    for line in sys.stdin:
        try:
            data_array = [_.replace("\\N", "") for _ in line.strip().split('\t')]

            export_data("", "", "", '')

        except Exception, e:
            erro_info = traceback.format_exc().decode().replace("\t", "  ").replace("\n", "  ;;;;")
            export_data('', '', '',  erro_info)
