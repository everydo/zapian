# -*- encoding: utf-8 -*-

import os
import yaml

CONFIG_FILE = 'schema.yaml'

class Schema(object):
    """ The schema class is improve xapian fields
    """
    fields = None       # {'title': ’NS’ }

    attributes = None   # {'created':1 }

    def __init__(self, db_path):
        self.db_path = db_path
        self.fields = {}
        self.attributes = {}
        self.load()

    def get_prefix(self, name, auto_add=True):
        """ get prefix by field name
        """
        prefix = self.fields.get(name)
        if prefix is None and auto_add:
            prefix = self.add_field(name)
        return prefix

    def get_slot(self, name, auto_add=True):
        """ get slot by attribute name
        """
        slot = self.attributes.get(name)
        if slot is None and auto_add:
            slot = self.add_attribute(name)
        return slot

    def load(self):
        """ load fields and attributes from the yaml file """
        schema_path = os.path.join(self.db_path, CONFIG_FILE)
        if os.path.isfile(schema_path):
            with open(schema_path,'rb') as schema_file:
                schema = yaml.load(schema_file)

            self.fields = schema['fields']
            self.attributes = schema['attributes']

    def dump(self):
        """ dump fields and attributes into the yaml file """
        schema_path = os.path.join(self.db_path, CONFIG_FILE)
        schema = {'fields': self.fields, 'attributes': self.attributes}
        with open(schema_path,'wb') as schema_file:
            schema_file.write(yaml.dump(schema))

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
        return 'X' + '@'*multiple + chr(index+64)

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

    def split_term(self, term):
        if term[0] == 'Q':
            return 'Q', term[1:]

        if term[0] != 'X':
            return '', term

        for index in range(1, len(term)):
            if term[index] != '@':
                break
        index += 1

        return term[:index], term[index:]



