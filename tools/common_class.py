# coding: utf-8
# __author__: u"John"


class ADict(dict):
    def __setattr__(self, key, value):
        self[key] = value
        return

    def __getattr__(self, item):
        try:
            return self[item]
        except KeyError:
            raise AttributeError

