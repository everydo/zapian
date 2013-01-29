#encoding=utf-8

import os
import sys
import shutil
import cPickle as pickle
import tempfile
import unittest
import logging
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

        #engine.add_document(site_name     = self.site_name,
        #                    catalog_name  = self.catalog_name,
        #                    part_name     = self.parts[0],
        #                    uid           = 123456, 
        #                    doc           = self.doc
        #                    )

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

    def test_add_document(self, uid='123456', part=None):
        part = part if part is not None else self.parts[0]
        self.zapian.add_field('title')
        self.zapian.add_field('subjects')
        self.zapian.add_attribute('created')
        # test add document
        self.zapian.add_document(part, uid=uid, 
                index=self.doc, data={'data': "测试内容"},
                flush=True)
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

    def test_del_document(self):
        part = self.parts[0]
        # add a document 
        self.test_add_document(uid='123456', part=part)
        # delete the document
        self.zapian.delete_document(part, uids=['123456'], flush=True)
        # test get the document, it will be raise KeyError
        try:
            _get_document(_get_read_db(self.data_root, [part]), '123456')
            raise AssertionError("Unique ID '123456' is exists")
        except KeyError:
            pass

    def test_search_document(self):
        pass

if __name__ == '__main__':

    unittest.main()

