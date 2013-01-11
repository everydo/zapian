# -*- encoding: utf-8 -*-

import os
import json

class Schema(object):
    """ 索引目录, 支持字段

    暂不支持分区的自定义，只能根据path字段分区
    """
    fields = {}       # full-text search 字段 term
                      # {'title':{'prefix’:’NS’, ‘type’:’text|exact|multi’} }

    attributes = {}   # 属性 value
                      # {'created':{'slot':1, 'type':’int|float|timestamp|string’}, }

    parts = []

    def __init__(self, db_path):
        self.db_path = db_path
        self.load()

    def get_prefix(self, name):
        """ 得到 field 的 prefix 
            没有会自动生成
        """
        if name in self.fields:
            return self.fields[name]['prefix']

    def get_slot(self, name):
        """ 得到 attributes 的 slot 
            没有会自动生成
        """
        if name in self.attributes:
            return self.attributes[name]['slot']

    def load(self):
        """ load fields and attributes from the json file """
        schema_path = os.path.join(self.db_path, 'schema.json')
        if os.path.isfile(schema_path):
            with open(schema_path,'rb') as schema_file:
                schema = json.loads(schema_file.read())

            self.fields = schema['fields']
            self.attributes = schema['attributes']
            self.parts = schema['parts']

    def dump(self):
        """ dump fields and attributes into the json file """
        schema_path = os.path.join(self.db_path, 'schema.json')
        schema = {'fields': self.fields, 'attributes': self.attributes, 'parts': self.parts}
        with open(schema_path,'wb') as schema_file:
            schema_file.write(json.dumps(schema))

    def add_field(self, name):
        if name not in self.fields:
            prefix = self._gen_prefix()
            # XXX type need  be remove
            self.fields[name] = {'prefix': prefix, 'type': 'text'}
            return True
        else:
            return False

    def _gen_prefix(self):
        """ new prefix """
        count = len(self.fields) + 1
        multiple = count/24
        index = count % 24
        return 'X' + 'A'*multiple + chr(index+64)

    def add_attribute(self, name):
        if name not in self.attributes:
            slot = self._gen_slot()
            # XXX type need  be remove
            self.attributes[name] = {'slot': slot, 'type':'float'}
            return True
        else:
            return False

    def _gen_slot(self):
        """ new slot """
        return len(self.attributes) 

    def add_part(self, name):
        """  """
        # FIXME 是否需要倚赖xapian, 直接在这里创建数据库?
        if name not in self.parts:
            self.parts.append(name)
            return True
        else:
            return False

    def remove_part(self, name):
        if name not in self.parts:
            self.parts.remove(name)
            return True
        else:
            return False

    def get_part_path(self, name):
        return self.parts.get(name, None)

