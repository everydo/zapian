zapian: a schemaless wrap for xapian
===============================================

喜欢elasticsearch的api，但是憎恨Luence的java架构，憎恨开启新的服务进程？

那么zapian，可能是你需要的....

特性
=============

- 更友好的schemaless的api
- 支持分区索引：可单独指定分区搜索，或合并搜索

  - 历史数据存放在不同的索引分区
  - 根据数据存放区域进行分区

Schemaless API
====================
首先需要初始化数据库:

      db = get_zapian_db(root='/tmp/test_zapian_db')

添加一个分区：

      db.add_part('2001-02')
      db.list_parts()

添加索引:

      db.add_document(part='default', 
                      uid='1111', 
                      index = { '+title' : u'我们很好.doc', 
		                'searchable_text' : u'', 
                                'modified' : datetime.datetime(), 
                                'crated' : datetime.datetime()
                            },
                       data = {}
                      )

修改索引:

      db.replace_document(part, uid, doc)

删除索引:

      db.delete_document(part, uid)

doc和索引的关系
=======================
xapian内部对数据有三种用途：term索引、排序字段、返回data；系统自动对数据类型进行处理：

- set/list/tuple：对每个包含数据，完整匹配搜索(term索引)
- string/unicode: 用于全文搜索(term索引)
- datetime/int/float: 用于排序和范围比较(排序字段)
- 如果字符串类型的字段以 + 开头，表示除了全文索引，也会用于排序

数据库的结构
===================
每个数据库内部有个schema.json, 有系统自动维护, 记录了3个信息：

1) PREFIX和字段的映射： title':"NC", 'created':"LL"
2) SLOT: ['modified', 'created']
3) part: 20120112  最新分区

文件夹结构：

          schema.json
          20120112/
          20120512/

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

