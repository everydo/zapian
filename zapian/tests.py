#encoding=utf-8

import os
import shutil
import cPickle as pickle
import tempfile
import unittest
import json
from datetime import datetime

from api import Zapian, _get_read_db, _get_document

class ZapianTest(unittest.TestCase):

    def setUp(self):
        # init the database attributes
        self.data_root = tempfile.mkdtemp()
        self.parts = ['part01', 'part02']

        # init the database environ
        if not os.path.exists(self.data_root):
            print 'create %s database' % self.data_root
            os.makedirs(self.data_root)

        self.zapian = Zapian(self.data_root)
        for part_name in self.parts:
            database_path = os.path.join(self.data_root, part_name)
            if not os.path.isdir(database_path):
                os.makedirs(database_path)
            self.zapian.add_part(part_name)

        # init the test data
        self.doc = {'title':'we are firend', 
                    'subjects':['file','ktv','what@gmail.com'], 
                    'created':datetime(2000, 1, 1)}

    def tearDown(self):
        """ """
        if os.path.exists(self.data_root):
            shutil.rmtree(self.data_root)

        self.zapian.fields.clear()
        self.zapian.attributes.clear()

    def test_schema(self):
        schema = self.zapian
        # test gen prefix and slot
        prefix = schema._gen_prefix() 
        self.assertEqual(prefix, 'XA')
        slot = schema._gen_slot()
        self.assertEqual(slot, 0)
        # test add field and attribute
        new_prefix = schema.add_field('new_field')
        self.assertTrue(schema.get_prefix('new_field') == new_prefix == prefix)
        new_slot = schema.add_attribute('new_attribute')
        self.assertTrue(schema.get_slot('new_attribute') == new_slot == slot)
        # test load the old schema
        new_zapian = Zapian(self.data_root)
        self.assertEqual(new_zapian.fields, {'new_field':'XA'})
        self.assertEqual(new_zapian.attributes, {'new_attribute':0})

    def _add_document(self, uid, part=None, doc=None):
        part = part if part is not None else self.parts[0]
        self.zapian.add_field('title')
        self.zapian.add_field('subjects')
        self.zapian.add_attribute('created')
        # test add document
        self.zapian.add_document(part, uid=uid, 
                index=doc or self.doc, data={'data': "测试内容"},
                flush=True)

    def test_add_document(self):
        part = self.parts[0]
        uid = "12345"
        self._add_document(uid, part)
        # test value of the new document
        doc = _get_document(_get_read_db(self.data_root, [part]), uid)
        for value in doc.values():
            if value.num == 0:
                self.assertEqual(value.value, '946656000')
        # test term of the new document
        validate_terms = ['XAare', 'XAfirend', 'XAwe', 'XBcom', 'XBfile', 'XBktv', 'XBwhat_gmail', 'Q'+uid]
        old_terms = [ term.term for term in doc.termlist() ]
        self.assertTrue(len(validate_terms), len(old_terms))
        self.assertEqual(set(validate_terms), set(old_terms))
        # test data of the new document
        data = pickle.loads( doc.get_data() )['data']
        self.assertEqual(data, '测试内容')

    def test_update_document(self):
        part = self.parts[0]
        uid = "12345"
        # add a document 
        self._add_document(uid=uid, part=part)
        new_doc = self.doc.copy()
        new_doc['title'] = "new title"

        self.zapian.update_document(part, uid=uid, index=new_doc, flush=True)
        # test value of the new document
        doc = _get_document(_get_read_db(self.data_root, [part]), uid)
        for value in doc.values():
            if value.num == 0:
                self.assertEqual(value.value, '946656000')
        # test term of the new document
        validate_terms = ['XAnew', 'XAtitle', 'XBcom', 'XBfile', 'XBktv', 'XBwhat_gmail', 'Q'+uid]
        old_terms = [term.term for term in doc.termlist()]
        self.assertTrue(len(validate_terms), len(old_terms))
        self.assertEqual(set(validate_terms), set(old_terms))
        # test data of the new document
        data = pickle.loads( doc.get_data() )['data']
        self.assertEqual(data, '测试内容')

    def test_replace_document(self):
        part = self.parts[0]
        uid = "12345"
        # add a document 
        self._add_document(uid=uid, part=part)
        new_doc = self.doc.copy()
        new_doc['title'] = "new title"
        self.zapian.add_field('new-field')
        new_doc['new-field'] = 'last'

        self.zapian.replace_document(part, uid=uid, index=new_doc, flush=True)
        # test value of the new document
        doc = _get_document(_get_read_db(self.data_root, [part]), uid)
        for value in doc.values():
            if value.num == 0:
                self.assertEqual(value.value, '946656000')
        # test term of the new document
        validate_terms = ['XAnew', 'XAtitle', 'XBcom', 'XBfile', 'XBktv', 'XBwhat_gmail', 'XClast', 'Q'+uid]
        old_terms = [term.term for term in doc.termlist()]
        self.assertTrue(len(validate_terms), len(old_terms))
        self.assertEqual(set(validate_terms), set(old_terms))
        # test data of the new document
        self.assertEqual(doc.get_data(), '')

    def test_del_document(self):
        part = self.parts[0]
        uid = "12345"
        # add a document 
        self._add_document(uid=uid, part=part)
        # delete the document
        self.zapian.delete_document(part, uids=[uid], flush=True)
        # test get the document, it will be raise KeyError
        try:
            _get_document(_get_read_db(self.data_root, [part]), uid)
            raise AssertionError("Unique ID '%s' is exists" % uid)
        except KeyError:
            pass

    def test_search_document(self):
        part = self.parts[0]
        uid = "12345"
        # add a document 
        self._add_document(uid=uid, part=part)
        # serach
        query_str = json.dumps({'filters': [  
                                                [[u'title'], u'we', u'parse'],
                                                [u'subjects', u'file ktv', u'anyof'],
                                ] })
        results = self.zapian.search([part], query_str, start=0, stop=10)

        self.assertEqual([uid], results)

    def test_search_document_for_mulit_database(self):
        """ """
        # add first document into first database
        first_uid = "12345"
        first_doc = {'title':'we are firend', 
                    'subjects':['file','ktv','what@gmail.com'] 
                    }
        self._add_document(uid=first_uid, part=self.parts[0], doc=first_doc)
        # add second document into second database
        second_uid = '67890'
        second_doc = {'title':'Go to school', 
                    'subjects':['morning','walking','sport'] 
                    }
        self._add_document(uid=second_uid, part=self.parts[1], doc=second_doc)
        # add third document into first database
        third_doc = {'title': 'Big Data',
                    'subjects': ['big', 'expensive']}
        self._add_document(uid="45678", part=self.parts[0], doc=third_doc)

        # search for muilt database

        # search firrst document
        query_str = json.dumps({'filters': [  
                                                [[u'title'], u'we', u'parse'],
                                                [u'subjects', u'file ktv', u'anyof'],
                                ] })
        results = self.zapian.search(self.parts, query_str, start=0, stop=10)
        self.assertEqual([first_uid], results)

        # search second document
        query_str = json.dumps({'filters': [  
                                                [[u'title'], u'Go', u'parse'],
                                                [u'subjects', u'walking sport', u'anyof'],
                                ] })
        results = self.zapian.search(self.parts, query_str, start=0, stop=10)
        self.assertEqual([second_uid], results)

        # search two document
        query_str = json.dumps({'filters': [  
                                                [u'title', u'Go we', u'anyof'],
                                                [u'subjects', u'walking sport ktv', u'anyof'],
                                ] })
        results = self.zapian.search(self.parts, query_str, start=0, stop=10)
        self.assertEqual(len(results), 2)
        self.assertEqual(set([first_uid, second_uid]), set(results))

if __name__ == '__main__':
    unittest.main()

