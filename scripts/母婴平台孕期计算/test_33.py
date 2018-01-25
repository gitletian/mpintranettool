# coding: utf-8
from __future__ import unicode_literals

import sys
if __name__ == '__main__':
    for line in sys.stdin:
        channel, subject = line.strip().split('\t')
        print "\t".join([channel, subject])

