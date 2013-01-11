# -*- encoding: utf-8 -*-

""" 索引引擎 （核心api）

1. 根据catalog字段自动分区
2. 理解document的层次关系根据path来判断)
3. 根据查询条件，字段选择分区

分区策略：一个特殊的path字段来分区，如果在同一根文件夹，则位于同一分区
"""

import xapian_driver as IR
from utils import process_doc

import logging
logger = logging.getLogger('zapian')

class Zapian:
    def __init__(self, db_path):
        self.db_path = db_path
        
    def add_document(part_name, uid, index, data=None, flush=True, **kw):
        """ 增加一个索引
        doc : {'title':'asdfa asdfa asdf', 'tags':['asdf','asdfa','asdf'], 'created':12312.23}
        """
        IR.add_document(site_name, catalog_name, part_name, uid,
                process_doc(catalog_name, doc), flush, **kw)
    
    def replace_document(part_name, uid, index, data=None, flush=True, **kw):
        """ 重建文档索引 """
        IR.replace_document(site_name, catalog_name, part_name, uid,
                process_doc(catalog_name, doc), flush, **kw)
    
    def delete_document(part_name, uids, flush=True, **kw):
        """ 删除一个文档 """
        IR.delete_document(site_name, catalog_name, part_name, uids, flush, **kw)
    
    def remove_part(part_name):
        """ 删除一个分区 """
    
        IR.remove_part(site_name, catalog_name, part_name,)
    
    def update_document(part_name, uid, index, data=None, flush=True, **kw):
        """ 更改文档某个字段的索引
        注意，xapian不支持，这里手工处理 """
    
        IR.update_document(site_name, catalog_name, part_name, uid, 
                process_doc(catalog_name, doc), flush, **kw)

    def flush(part_name, **kw):
        """ """
        IR.commit(site_name, catalog_name, part_name, **kw)
    
    # 搜索
    def search(parts, query_json):
        """ 搜索查询结果 """
        # 如果没有指定数据库位置，就搜索这个catalog下所有的数据库
    
        orderby = query_set._order_by
        start = query_set._start
        stop = 0
        if query_set._limit and query_set._limit < 100000:
            stop =  start + query_set._limit
        else:
            # 取出数据太大，会导致内存分配错误
            stop = start + 100000
    
        return IR.search(site_name = site_name,
                        catalog_name = catalog_name, 
                        parts = parts, 
                        query_str = query_set.json(),
                        orderby = orderby,
                        start = start,
                        stop = stop,)
