#encoding=utf-8

import os
import shutil
import cPickle as pickle
import tempfile
import unittest
from datetime import datetime

from api import Zapian
from query import Query

# not cached only read database connection of xapian
import api
api.READ_DB_REFRESH_DELTA = 0

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

        # init the test data
        self.doc = {'+title':'we are firend', 
                    'subjects':['file','ktv','what@gmail.com'], 
                    'created':datetime(2000, 1, 1)}

    def tearDown(self):
        """ """
        if os.path.exists(self.data_root):
            shutil.rmtree(self.data_root)

        self.zapian.schema.fields.clear()
        self.zapian.schema.attributes.clear()

    def test_schema(self):
        schema = self.zapian.schema
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
        self.assertEqual(new_zapian.schema.fields, {'new_field':'XA'})
        self.assertEqual(new_zapian.schema.attributes, {'new_attribute':0})

    def test_add_document(self):
        part = self.parts[0]
        uid = "12345"
        self.zapian.add_document(part, uid=uid, 
                index=self.doc, data={'data': "测试内容"},
                flush=True)
        # test value of the new document
        doc = self.zapian._get_document(uid, [part])
        values = [v.value for v in doc.values()]
        self.assertEqual(set(values), set(['946656000', 'we are firend']))
        # test term of the new document
        title_prefix = self.zapian.schema.get_prefix('title', auto_add=False)
        subjects_prefix = self.zapian.schema.get_prefix('subjects', auto_add=False)
        validate_terms = ['Q'+uid,
                            title_prefix + 'are', 
                            title_prefix + 'firend', 
                            title_prefix + 'we',
                            subjects_prefix + 'file', 
                            subjects_prefix + 'ktv', 
                            subjects_prefix + 'what_gmail_com', ]
        old_terms = [ term.term for term in doc.termlist() ]
        self.assertEqual(len(validate_terms), len(old_terms))
        self.assertEqual(set(validate_terms), set(old_terms))
        # test data of the new document
        data = pickle.loads( doc.get_data() )['data']
        self.assertEqual(data, '测试内容')

    def test_update_document(self):
        part = self.parts[0]
        uid = "12345"
        # add a document 
        self.zapian.add_document(part, uid=uid, 
                index=self.doc, data={'data': "测试内容"},
                flush=True)
        new_doc = self.doc.copy()
        new_doc['+title'] = "new title"

        self.zapian.update_document(part, uid=uid, index=new_doc, flush=True)
        # test value of the new document
        doc = self.zapian._get_document(uid, [part])
        values = [v.value for v in doc.values()]
        self.assertEqual(set(values), set(['946656000', 'new title']))
        # test term of the new document
        title_prefix = self.zapian.schema.get_prefix('title', auto_add=False)
        subjects_prefix = self.zapian.schema.get_prefix('subjects', auto_add=False)
        validate_terms = ['Q'+uid,
                            title_prefix + 'new', 
                            title_prefix + 'title', 
                            subjects_prefix + 'file', 
                            subjects_prefix + 'ktv', 
                            subjects_prefix + 'what_gmail_com', ]
        old_terms = [term.term for term in doc.termlist()]
        self.assertTrue(len(validate_terms) == len(old_terms))
        self.assertEqual(set(validate_terms), set(old_terms))
        # test data of the new document
        data = pickle.loads( doc.get_data() )['data']
        self.assertEqual(data, '测试内容')

    def test_replace_document(self):
        part = self.parts[0]
        uid = "12345"
        # add a document 
        self.zapian.add_document(part, uid=uid, 
                index=self.doc, data={'data': "测试内容"},
                flush=True)
        new_doc = self.doc.copy()
        new_doc['+title'] = "new title"
        self.zapian.schema.add_field('new-field')
        new_doc['new-field'] = 'last'

        self.zapian.replace_document(part, uid=uid, index=new_doc, flush=True)
        # test value of the new document
        doc = self.zapian._get_document(uid, [part])
        values = [v.value for v in doc.values()]
        self.assertEqual(set(values), set(['946656000', 'new title']))
        # test term of the new document
        title_prefix = self.zapian.schema.get_prefix('title', auto_add=False)
        subjects_prefix = self.zapian.schema.get_prefix('subjects', auto_add=False)
        new_field_prefix = self.zapian.schema.get_prefix('new-field', auto_add=False)
        validate_terms = ['Q'+uid,
                            new_field_prefix + 'last',
                            title_prefix + 'new', 
                            title_prefix + 'title', 
                            subjects_prefix + 'file', 
                            subjects_prefix + 'ktv', 
                            subjects_prefix + 'what_gmail_com', ]
        old_terms = [term.term for term in doc.termlist()]
        self.assertTrue(len(validate_terms) == len(old_terms))
        self.assertEqual(set(validate_terms), set(old_terms))
        # test data of the new document
        self.assertEqual(doc.get_data(), '')

    def test_del_document(self):
        part = self.parts[0]
        uid = "12345"
        # add a document 
        self.zapian.add_document(part, uid=uid, 
                index=self.doc, data={'data': "测试内容"},
                flush=True)
        # delete the document
        self.zapian.delete_document(part, uids=[uid], flush=True)
        # test get the document, it will be raise KeyError
        try:
            self.zapian._get_document(uid, [part])
            raise AssertionError("Unique ID '%s' is exists" % uid)
        except KeyError:
            pass

    def test_search_document(self):
        part = self.parts[0]
        uid = "12345"
        # add a document 
        self.zapian.add_document(part, uid=uid, 
                index=self.doc, data={'data': "测试内容"},
                flush=True)
        # serach
        query = [  
                    [[u'title'], u'we', u'parse'],
                    [u'subjects', u'file ktv', u'anyof'],
                ] 
        results = self.zapian.search([part], query)
        self.assertEqual([uid], results)

    def test_search_document_for_mulit_database(self):
        """ """
        # add first document into first database
        first_uid = "12345"
        first_doc = {'title':'we are firend', 
                    'subjects':['file','ktv','what@gmail.com'] 
                    }
        self.zapian.add_document(self.parts[0], uid=first_uid, 
                index=first_doc, data={'data': "测试内容"},
                flush=True)
        # add second document into second database
        second_uid = '67890'
        second_doc = {'title':'Go to school', 
                    'subjects':['morning','walking','sport'] 
                    }
        self.zapian.add_document(self.parts[1], uid=second_uid, 
                index=second_doc, data={'data': "测试内容"},
                flush=True)
        # add third document into first database
        third_doc = {'title': 'Big Data',
                    'subjects': ['big', 'expensive']}
        self.zapian.add_document(self.parts[0], uid="45678", 
                index=third_doc, data={'data': "测试内容"},
                flush=True)

        # search for muilt database

        # search firrst document
        query = [  
                    [[u'title'], u'we', u'parse'],
                    [u'subjects', u'file ktv', u'anyof'],
                 ] 
        results = self.zapian.search(self.parts, query)
        self.assertEqual([first_uid], results)

        # search second document
        query = [  
                    [[u'title'], u'Go', u'parse'],
                    [u'subjects', u'walking sport', u'anyof'],
                ]
        results = self.zapian.search(self.parts, query)
        self.assertEqual([second_uid], results)

        # search two document
        query = [  
                    [u'title', u'Go we', u'anyof'],
                    [u'subjects', u'walking sport ktv', u'anyof'],
                ]
        results = self.zapian.search(self.parts, query)
        self.assertEqual(len(results), 2)
        self.assertEqual(set([first_uid, second_uid]), set(results))

    def test_search_document_with_exclude(self):
        self.zapian.add_document(self.parts[0], uid="12345", 
                index=self.doc, flush=True)

        new_doc = self.doc.copy()
        new_doc['title'] = 'new title'
        self.zapian.add_document(self.parts[0], uid="12346", 
                index=new_doc, flush=True)

        query = Query(self.zapian.schema)
        query.filter('subjects', u'file', 'anyof')
        results = self.zapian.search([self.parts[0]], query_obj=query)
        self.assertEqual(set(results), set(['12345', '12346']))

        query.exclude('title', u'new', 'anyof')
        results = self.zapian.search([self.parts[0]], query_obj=query)
        self.assertEqual(results, ['12345'])

    def test_multi_query(self):
        self.zapian.add_document(self.parts[0], uid="12345", 
                index=self.doc, flush=True)

        new_doc = self.doc.copy()
        new_doc['+title'] = 'we talk'
        self.zapian.add_document(self.parts[0], uid="12346", 
                index=new_doc, flush=True)

        query1 = Query(self.zapian.schema)
        query1.filter('title', u'we talk', 'anyof')
        query2 = Query(self.zapian.schema)
        query2.filter('title', u'are', '')

        combined = query2 & query1
        results = self.zapian.search([self.parts[0]], query_obj=combined)
        self.assertEqual(results, ['12345'])

        combined = query2 | query1
        results = self.zapian.search([self.parts[0]], query_obj=combined)
        self.assertEqual(set(results), set(['12345', '12346']))

if __name__ == '__main__':
    unittest.main()

