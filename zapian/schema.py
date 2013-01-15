# -*- encoding: utf-8 -*-

import os
import json

class Schema(object):
    """ The schema class is improve xapian fields
    """
    fields = {}       # {'title':{'prefix’:’NS’, ‘type’:’text|exact|multi’} }

    attributes = {}   # {'created':{'slot':1, 'type':’int|float|timestamp|string’}, }

    def __init__(self, db_path):
        self.db_path = db_path
        self.load()

    def get_prefix(self, name):
        """ get prefix by field name
        """
        if name in self.fields:
            return self.fields[name]

    def get_slot(self, name):
        """ get slot by attribute name
        """
        if name in self.attributes:
            return self.attributes[name]

    def load(self):
        """ load fields and attributes from the json file """
        schema_path = os.path.join(self.db_path, 'schema.json')
        if os.path.isfile(schema_path):
            with open(schema_path,'rb') as schema_file:
                schema = json.loads(schema_file.read())

            self.fields = schema['fields']
            self.attributes = schema['attributes']

    def dump(self):
        """ dump fields and attributes into the json file """
        schema_path = os.path.join(self.db_path, 'schema.json')
        schema = {'fields': self.fields, 'attributes': self.attributes}
        with open(schema_path,'wb') as schema_file:
            schema_file.write(json.dumps(schema))

    def add_field(self, name):
        if name not in self.fields:
            prefix = self.fields[name] = self._gen_prefix()
            self.dump()
            return prefix
        else:
            return self.fields[name]

    def _gen_prefix(self):
        """ new prefix """
        count = len(self.fields) + 1
        multiple = count/24
        index = count % 24
        return 'X' + 'A'*multiple + chr(index+64)

    def add_attribute(self, name):
        if name not in self.attributes:
            slot = self.attributes[name] = self._gen_slot()
            self.dump()
            return slot
        else:
            return self.attributes[name]

    def _gen_slot(self):
        """ new slot """
        return len(self.attributes) 

