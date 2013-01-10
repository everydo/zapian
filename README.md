zapian: a schemaless wrap for xapian
===============================================

喜欢elasticsearch的api，但是憎恨lunce的java架构，憎恨开启新的服务进程？

那么zapian，可能是你需要的....

特性
=============

- schemaless的api
- 封装多区索引

================
索引和搜索
================

本模块类似xappy，区别：

1. 支持分区索引
2. 支持异步索引
3. 更友好的API

准备catalog
=====================

定义分区策略
---------------------------
直接按照根路径来分区::

   >>> def get_part(raw_doc):
   ...     return raw_doc.get('path', [''])[0]

   >>> def get_parts(conditions):
   ...     return [path[0] for path in conditions.get('path', [['',]])]

创建Catalog数据字典
-----------------------
包括fields/attributes, 分区策略::

  >>> from zapian import Catalog, catalog_registry
  >>> catalog = Catalog(fields = {
  ...                      'title':{'prefix’:’NS’, ‘type’:’text’}, 
  ...                      'searchable_text':{'prefix’:’TT’, ‘type’:’text’}, 
  ...                      'full_text':{'prefix’:’LA’, ‘type’:’text’}, 
  ...                      'uid':{'prefix’:’MM’, ‘type’:’exact’}, 
  ...                      'path':{'prefix’:’PT’, ‘type’:’multi’}, 
  ...                      'interfaces':{'prefix’:’CC’, ‘type’:’multi’}, 
  ...                      'tags':{'prefix’:’LA’, ‘type’:’multi’}, 
  ...                      },
  ...                      attributtes=={
  ...                      'created':{'slot':1, 'type':’timestamp’}, 
  ...                      'modified':{'slot':2, 'type':’timestamp’}, 
  ...                      'size':{'slot':3, 'type':’int’}, 
  ...                      'amount':{'slot':4, 'type':’float’}, 
  ...                      },  
  ...                      get_part, 
  ...                      get_parts)


注册Catalog
---------------------
注册::

  >>> catalog_registry[''] = catalog

索引
====================

首先需要初始化数据库::

  >>> from zapian import init_db, add_document, replace_document, delete_document
  >>> init_db(root='/tmp')

添加索引::

  >>> import datetime
  >>> site_name = 'default.zopen.test'
  >>> add_document(site_name, '', '1111', 
  ...              {'title':'我们很好.doc', 'searchable_text':'', 
  ...              'modified': datetime.datetime(), 'crated': datetime.datetime()},
  ...               async=False)

修改索引::

  >>> replace_document(site_name, catalog_name, uid, doc, async=False)

删除索引::

  >>> delete_document(site_name, catalog_name, part_name, uid, async=False)

搜索
==========
::

    >>> from zapian import QuerySet
    >>> QuerySet('', site_name).count()
    2
    >>> QuerySet(catalog, conn)[1]
    <SearchResult(rank=1...

    >>> # 对于全文索引字段简单的filter 不是全文匹配的,只要出现即可匹配
    >>> QuerySet(catalog, conn).filter(title="hello").count()
    2
    >>> # exclude 查询
    >>> QuerySet(catalog, conn).filter(title="hello").exclude(total_size="5").count()
    0

    >>> QuerySet(catalog, conn).filter(object_provides=["zopen.indexer.interfaces.ICatalogable"]).exclude(title='world').count()
    1

    >>> QuerySet(catalog, conn).filter(total_size="5").exclude(title="hello world").count()
    1

    >>> # unique 操作
    >>> QuerySet(catalog, conn).filter(title="hello").unique('total_size').count()
    1

    >>> QuerySet(catalog, conn, limit=1).filter(title="hello").count()
    1

    >>> # 排序
    >>> QuerySet(catalog, conn).filter(title="hello").sort('title').all()
    [<SearchResult(rank=0, id='...', data={'total_size': ['5'], 'title': [u'1...

    >>> # 倒序
    >>> QuerySet(catalog, conn).filter(title="hello").sort('-title').all()
    [<SearchResult(rank=0, id='...', data={'total_size': ['5'], 'title': [u'2...

    >>> QuerySet(catalog, conn).filter(title="hello").sum('total_size')
    10

    >>> QuerySet(catalog, conn).filter(title="hello baha").count()
    1

    >>> # __anyof 查询用来查询满足其中之一或多个条件的结果
    >>> QuerySet(catalog, conn).filter(title="hello").filter(total_size=5).count()
    2
    >>> QuerySet(catalog, conn).filter(title="hello").filter(total_size__range=(2, 10)).count()
    2
    
    >>> # 默认不加任何后缀的查询为 anyof
    >>> QuerySet(catalog, conn).filter(object_provides=["zopen.indexer.interfaces.ICatalogable"]).count()
    2

    >>> QuerySet(catalog, conn).filter(object_provides=["zopen.indexer.interfaces"]).count()
    0

    >>> # allof 用来
    >>> QuerySet(catalog, conn).filter(object_provides__allof=["zopen_indexer_interfaces_ICatalogable"]).count()
    2

    >>> QuerySet(catalog, conn).filter(object_provides__anyof=["zopen_indexer_interfaces_ICatalogable", "NO_SUCH"]).count()
    2
    >>> QuerySet(catalog, conn).filter(object_provides__allof=["zopen_indexer_interfaces_ICatalogable", "NO_SUCH"]).count()
    0


组合查询
--------------------

    >>> # 组合查询
    >>> qs = (QuerySet(catalog, conn).filter(title="hello baha") | QuerySet(catalog, conn).filter(title="hello world"))
    >>> qs.count()
    2


Xapian Hack
===========

当只提供单个索引的文档时，不重建其它索引::

    >>> conn_2 = xappy.IndexerConnection('test_1')
    >>> conn_2.add_field_action('title', xappy.FieldActions.INDEX_FREETEXT)
    >>> conn_2.add_field_action('body', xappy.FieldActions.INDEX_FREETEXT)
    >>> conn_2.add_field_action('desc', xappy.FieldActions.INDEX_FREETEXT)
    >>> conn_2.add_field_action('title', xappy.FieldActions.STORE_CONTENT)
    >>> conn_2.add_field_action('body', xappy.FieldActions.STORE_CONTENT)

    >>> doc = xappy.UnprocessedDocument()
    >>> doc.fields.append(xappy.Field('title', "First Doc"))
    >>> doc.fields.append(xappy.Field('body', "First Body"))
    >>> doc.fields.append(xappy.Field('desc', "First Desc"))
    >>> root['doc1'] = doc
    >>> conn_2.add(doc)
    '0'

    >>> doc_2 = xappy.UnprocessedDocument()
    >>> doc_2.fields.append(xappy.Field('title', "Second Doc"))
    >>> doc_2.fields.append(xappy.Field('body', "Second Body"))
    >>> doc_2.fields.append(xappy.Field('desc', "Second Desc"))
    >>> root['doc2'] = doc_2
    >>> conn_2.add(doc_2)
    '1'

    >>> doc_3 = xappy.UnprocessedDocument()
    >>> doc_3.fields.append(xappy.Field('title', "Third Doc"))
    >>> doc_3.fields.append(xappy.Field('body', "Third Body"))
    >>> doc_3.fields.append(xappy.Field('desc', "Third Desc"))
    >>> conn_2.add(doc_3)
    '2'

    >>> conn_2.flush()

    # 只有一个结果
    >>> se = xappy.SearchConnection('test_1')
    >>> q = se.query_field('title', 'Second')
    >>> se.search(q, 0, 100)
    <SearchResults(startrank=0, endrank=1...

    >>> doc_2.id = '1'
    >>> doc_2.fields[1]
    Field('body', 'Second Body')

    >>> doc_2.fields[1].value="Third Body"
    >>> conn_2.replace(doc_2)
    >>> conn_2.flush()

    >>> se.close()

    # 有两个结构
    >>> se = xappy.SearchConnection('test_1')
    >>> q = se.query_field('body', 'Third')
    >>> se.search(q, 0, 100)
    <SearchResults(startrank=0, endrank=2...

    # 确保title 字段没有重建索引, 若失败则表明没有Hack Xapian！！
    >>> q = se.query_field('title', 'Second')
    >>> se.search(q, 0, 100)
    <SearchResults(startrank=0, endrank=1...

    >>> se.search(q, 0, 100)[0].data
    {'body': ['Third Body'], 'title': ['Second Doc']}

