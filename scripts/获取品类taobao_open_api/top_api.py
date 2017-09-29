# -*- coding: utf-8 -*-
from __future__ import unicode_literals
import urllib
import hashlib
import datetime
import json


class Api:
    def __init__(self, gateway='http://gw.api.taobao.com/router/rest',
                 secret="374a7a9a5b328df3dfsddf4ddd51b46975a0ecfcb",
                 session=None,
                 **kwargs):
        """
        top_api 的初始化
        :param gateway:访问的网址;
        secret:密钥;
        session:访问的session;
        kwargs:其他参数
        :return:
        """
        self.default_params = dict(
            app_key='23307743',
            format='json',
            sign_method='md5',
            v='2.0',
            partner_id='tao_api_python_1.0',
            fields='',
            method='',
        )
        self.default_params.update(kwargs)
        self.secret = secret
        self.session = session
        self.gateway = gateway

    def sign(self, params):
        """
        对参数进行签名
        :param params:参数
        :return:
        """
        items = params.items()
        items.sort()
        s = self.secret
        for i in items:
            s += '%s%s' % i
        s += self.secret
        m = hashlib.md5()
        m.update(s)
        return m.hexdigest().upper()

    def execute(self):
        """
        获取请求结果
        :param session:session
        :return:
        """
        tmp_param = dict(timestamp=datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        if self.session:
            tmp_param.update(dict(session=self.session))

        tmp_param.update(self.default_params)
        data_string = urllib.urlencode(tmp_param)

        tmp_param["sign"] = self.sign(tmp_param)
        param_string = urllib.urlencode(tmp_param)
        url = '%s?%s' % (self.gateway, param_string)
        resp = None
        try:
            http = urllib.urlopen(url, data_string)
            resp = json.load(http)
            http.close()

            itemprops_get_response = resp["itemprops_get_response"]
            if not itemprops_get_response.has_key("item_props"):
                print "==={0}======={1}====".format(tmp_param["cid"], resp)
                return

            resp = itemprops_get_response["item_props"]
        except:
            print "--------------error-----------------{}".format(tmp_param["cid"])
        return resp


if __name__ == '__main__':
    param = dict(
        method="taobao.itemprops.get",
        fields="pid,name,must,multi,prop_values",
    )
    api = Api(secret="374a7a9dda5b328df3dfdss451b46975a0ecfcb", **param)

    api.default_params.update({"cid": "1623"})
    api.execute()



