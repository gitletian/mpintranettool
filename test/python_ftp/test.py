# coding: utf-8
# __author__: ""
from __future__ import unicode_literals
from ftplib import FTP

ftp = FTP()
ftp.connect(host='172.16.1.100', port=21)
ftp.login()
ftp.cwd("/pub/Data/media_choice")
print("====="+ftp.pwd())
bufsize=1204
print ftp.getwelcome()

ftp.retrlines("RETR new_txt.txt", bufsize)
with open('new_txt.txt', 'r') as f:
    s = f.readlines()
    print(s)


