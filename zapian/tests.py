#encoding=utf-8

import os
import sys
import shutil
import tempfile
import unittest
import logging
from datetime import datetime

#from zapian import (
#                    catalog, 
#                    queryset,
#                    engine,
#                    xapian_driver)

from schema import Schema

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
    
class ZapianTest(unittest.TestCase):

    def setUp(self):
        # init the database attributes
        self.data_root = os.path.join(tempfile.mkdtemp(), 'test_zapian')
        self.parts = ['part01', 'part02']

        # init the database environ
        if not os.path.exists(self.data_root):
            print 'create %s database' % self.data_root
            os.makedirs(self.data_root)

        for part_name in self.parts:
            database_path = os.path.join(self.data_root, part_name)
            if not os.path.isdir(database_path):
                os.makedirs(database_path)

        # init the test data
        self.doc = {'title':'we are firend', 
                    'subjects':['file','ktv','what'], 
                    'created':datetime.now()}

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

    def test_schema(self):
        schema = Schema(self.data_root)
        for part in self.parts:
            schema.add_part(part)
        # test gen prefix and slot
        prefix = schema._gen_prefix() 
        self.assertTrue(prefix == 'XA')
        slot = schema._gen_slot()
        self.assertTrue(slot == 0)
        # test add field and attribute
        new_prefix = schema.add_field('new_field')
        self.assertTrue(schema.get_prefix('new_field') == new_prefix == prefix)
        new_slot = schema.add_attribute('new_attribute')
        self.assertTrue(schema.get_slot('new_attribute') == new_slot == slot)

    def atest_remove_database(self):
        """ 测试删除数据库接口
        """
        engine.remove_part(self.site_name, self.catalog_name, self.parts[0])
        database_path = os.path.join(self.data_root, self.site_name, self.catalog_name, self.parts[0])
        self.assertTrue(not os.path.exists(database_path))
        for part_name in self.parts[1:]:
            database_path = os.path.join(self.data_root, self.site_name, self.catalog_name, part_name)
            self.assertTrue(os.path.exists(database_path))

    def atest_delete_document(self):
        """ 测试删除一个记录
        """
        engine.delete_document(site_name = self.site_name,
                                catalog_name = self.catalog_name,
                                part_name = self.parts[0],
                                uid = '123456',)
        self.assertRaises(AssertionError, self.test_search_for_one_database)

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
    if len(sys.argv) > 1:
        level_name = sys.argv.pop(1).upper()
    else:
        level_name = 'INFO'

    #logger = initlog(level_name)

    unittest.main()

