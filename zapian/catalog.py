# -*- encoding: utf-8 -*-

""" catalog管理: 索引字段信息

"""

class Catalog(object):
    """ 索引目录, 支持字段

    暂不支持分区的自定义，只能根据path字段分区
    """
    name = ''

    fields = {}       # full-text search 字段 term
                      # {'title':{'prefix’:’NS’, ‘type’:’text|exact|multi’} }

    attributes = {}   # 属性 value
                      # {'created':{'slot':1, 'type':’int|float|timestamp|string’}, }

    data = {}         # 需要返回的东西

    def __init__(self, name=None, fields=None, attributes=None, data=None, **argv):
        if name:
            self.name = name
        if fields:
            self.fields = fields
        if attributes:
            self.attributes = attributes
        if data:
            self.data = data

        for key, value in argv.items():
            setattr(self, key, value)

_catalog_registry = {}

def register_catalog(catalog):
    """ 注册catalog """
    _catalog_registry[catalog.name] = catalog

def get_catalog(name):
    """ 得到一个catalog """
    return _catalog_registry[name]

