zapian: a schemaless wrap for xapian
===============================================

喜欢elasticsearch的api，但是憎恨Luence的java架构，憎恨开启新的服务进程？

那么zapian，可能是你需要的....

特性
=============

- 更友好的schemaless的api
- 支持分区索引：更高的搜索性能

字段类型的处理
=================
- 字符串：全文索引
- 列表：完整匹配
- 整数、时间、小数：用于排序和条件判断

Schemaless API
====================
虽然schemaless，但是还是需要设置：1) PREFIX和字段的映射：2) SLOT

      schema = {"prefix":{'title':"NC", 'created':"LL"}, 
                "slots":['modified', 'created']
               }

首先需要初始化数据库:

      db = get_zapian_db(root='/tmp/test_zapian_db', schema)

添加索引:

      db.add_document(part='default', 
                      uid='1111', 
                      doc={ 'title' : '我们很好.doc', 
		            'searchable_text' : '', 
                            'modified' : datetime.datetime(), 
                            'crated' : datetime.datetime()
                            },
                     )

修改索引:

      db.replace_document(part, uid, doc)

删除索引:

      db.delete_document(part, uid)

分区搜索
================

- 归档数据和当前数据存放在不同的分区
- 不同区域的索引，分别存放
- 可合并搜索


搜索
==========

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
=============

      >>> # 组合查询
      >>> qs = (QuerySet(catalog, conn).filter(title="hello baha") | QuerySet(catalog, conn).filter(title="hello world"))
      >>> qs.count()
      2

