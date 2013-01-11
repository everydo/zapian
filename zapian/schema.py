# -*- encoding: utf-8 -*-

class Schema(object):
    """ 索引目录, 支持字段

    暂不支持分区的自定义，只能根据path字段分区
    """
    name = ''

    fields = {}       # full-text search 字段 term
                      # {'title':{'prefix’:’NS’, ‘type’:’text|exact|multi’} }

    attributes = {}   # 属性 value
                      # {'created':{'slot':1, 'type':’int|float|timestamp|string’}, }

    parts = {}

    def __init__(self, db_path):
	self.db_path = db_path
	self.load()

    def get_prefix(self, name, auto_add=True):
	pass

    def get_slot(self, name, auto_add=True):
	pass

    def load(self):
        pass

    def add_field(self, name):
        pass

    def add_attribute(self, name):
        pass

    def add_part(self, name)
        pass

    def remove_part(self, name):
        pass

    def get_part_path(self, name):
        pass

