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

    def atest_remove_database(self):
        """ 测试删除数据库接口
        """
        engine.remove_part(self.site_name, self.catalog_name, self.parts[0])
        database_path = os.path.join(self.data_root, self.site_name, self.catalog_name, self.parts[0])
        self.assertTrue(not os.path.exists(database_path))
        for part_name in self.parts[1:]:
            database_path = os.path.join(self.data_root, self.site_name, self.catalog_name, part_name)
            self.assertTrue(os.path.exists(database_path))

    def atest_search_for_one_database(self, qs=None, search_uid='123456'):
        """ 测试一个数据库搜索
        """
        if not qs:
            qs = queryset.QuerySet()
            qs.parse(u'we are', ['title'])
            qs.filter(subjects__anyof=['file', 'ktv'])
        result = engine.search(site_name = self.site_name,
                                catalog_name = self.catalog_name,
                                parts = self.parts[0],
                                query_set = qs,
                                )
        self.assertTrue(search_uid in result)

    def atest_search_for_multi_database(self):
        """ 测试多个个数据库联合搜索
        """
        engine.add_document(site_name     = self.site_name,
                            catalog_name  = self.catalog_name,
                            part_name     = self.parts[1],
                            uid           = 7890, 
                            doc           = self.doc
                            )
        qs = queryset.QuerySet()
        qs.parse(u'we are', ['title'])
        qs.filter(subjects__anyof=['file', 'ktv'])
        result = engine.search(site_name = self.site_name,
                                catalog_name = self.catalog_name,
                                parts = self.parts,
                                query_set = qs,
                                )
        self.assertTrue('7890' in result)
        self.assertTrue('123456' in result)

    def atest_index_for_multi(self):
        """ 测试一个数据库连续写入
            同一路径写数据一次只能打开一个
        """
        # init the test data
        self.doc = {'title':'missiong you', 
                    'subjects':['while','white','watch'], 
                    'created':datetime.now()}

        engine.add_document(site_name     = self.site_name,
                            catalog_name  = self.catalog_name,
                            part_name     = self.parts[0],
                            uid           = 123457, 
                            doc           = self.doc
                            )
        # 默认搜索 123456 uid 的文档
        self.assertEqual(self.test_search_for_one_database(), None)

        # 搜索 123457 uid 的文档
        qs = queryset.QuerySet()
        qs.parse(u'you', ['title'])
        qs.filter(subjects__allof=['while', 'white'])
        self.assertEqual(self.test_search_for_one_database(qs, '123457'), None)

        # 搜索错误的文档
        qs = queryset.QuerySet()
        qs.parse(u'we are', ['title'])
        qs.filter(subjects__allof=['while', 'white'])
        # 搜索不到, 会抛出assertion异常
        self.assertRaises(AssertionError, self.test_search_for_one_database, qs)

    def atest_replace_document(self):
        """ 测试重建文档索引
        """
        new_doc = {'title':'new doc',
                    'subjects':['new', 'doc'],
                    'created':datetime.now()}

        # 先正常搜索一次
        self.assertEqual(self.test_search_for_one_database(), None)
        # 重建文档索引
        engine.replace_document(site_name = self.site_name,
                                catalog_name = self.catalog_name,
                                part_name = self.parts[0],
                                uid = '123456',
                                doc = new_doc,)
        # 原来的应该搜索不到了, 就会抛出assertion异常
        self.assertRaises(AssertionError, self.test_search_for_one_database)

    def atest_update_document(self):
        """ 测试更改文档某个字段的索引
        """
        # 先正常搜索一次
        self.assertEqual(self.test_search_for_one_database(), None)
        engine.update_document(site_name = self.site_name,
                                catalog_name = self.catalog_name,
                                part_name = self.parts[0],
                                uid = '123456',
                                doc = {'title':'new title'},)
        # 原来的应该搜索不到了, 就会抛出assertion异常
        self.assertRaises(AssertionError, self.test_search_for_one_database)

if __name__ == '__main__':

    unittest.main()

