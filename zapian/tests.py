#encoding=utf-8

import os
import sys
import shutil
import cPickle as pickle
import tempfile
import unittest
import logging
import json
from datetime import datetime

from api import Zapian, _get_read_db, _get_document

def initlog(level_name='INFO'):
    """ """
    logger = logging.getLogger('zopen.haystack')
    hdlr = logging.StreamHandler(sys.stdout)
    #hdlr = logging.FileHandler('/tmp/xapian.txt')
    formatter = logging.Formatter('%(levelname)s %(message)s')
    hdlr.setFormatter(formatter)
    logger.addHandler(hdlr)
    logger.setLevel(logging.getLevelName(level_name))
    return logger

# 添加字段, field
fields = {
    # 创建人
    'creators':              'XX',
    # 标题
    'title':                 'XH',
    # 这个是关键字，或者tag
    'subjects':              'XL',
    # 创建时间
    'created':               'XU',
    'modified':              'XV',
    'effective':             'XS',
    'expires':               'XP',
    #贡献者
    'contributors':          'XB',
    'responsibles':          'XM',
    'responsible':           'XJ',
    'identifier':            'XR',
    # 状态
    'stati':                 'XN',
    'path':                  'XD',
    'parent':                'XO',
    # 开始时间
    'start':                 'XK',
    # 结束时间
    'end':                   'XG',
    # 总量，工作量
    'amount':                'XW',

    # 可查看的人
    'allowed_principals':    'XC',
    # 禁止查看的人
    'disallowed_principals': 'XA',
    # 对象提供的接口 Set
    'object_provides':       'XQ',
    #尺寸大小的索引
    'size':                  'XF',
    'total_size':            'XI',
    #检查人  Set
    'reviewer':              'XE',
    # 级别
    'level':                 'XT',
}

# 初始化索引, store_content
# 需要返回到结果，进行后续处理的(如统计报表)
data = ['title', 'size', 'total_size', 'amount', 'creators', 'stati', 'path', 'created']

# 需要做排序的字段, sortable
attributes = {
    'expires' :     0,
    'end' :         1,
    'effective' :   2,
    'created' :     3,
    'total_size' :  4,
    'title' :       5,
    'modified' :    6,
    'start' :       7,
    'amount' :      8,
    'createds' :    9,
    'identifier' :  10,
    'size' :        11,
    }

    
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
        termlist = ['XAare', 'XAfirend', 'XAwe', 'XBcom', 'XBfile', 'XBktv', 'XBwhat_gmail']
        termlist.append('Q'+uid)
        for term in doc.termlist():
            self.assertTrue(term.term in termlist)
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
        self.assertEqual(set([first_uid, second_uid]), set(results))

if __name__ == '__main__':
    unittest.main()

