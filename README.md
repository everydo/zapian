zapian: schemaless python interface to Xapian
===============================================

作为一个pythoner，我们有理由爱xapian... 

但xappy已年久失修过于陈旧了... 喜欢elasticsearch的api，但是憎恨Luence的java架构，不愿引入新的服务进程？

那么zapian，可能是你需要的....

欢迎拍砖： http://weibo.com/panjunyong

特性
=============

- 为xapian提供更友好的schemaless的api
- 支持分区索引：可单独指定分区搜索，或合并搜索

  - 历史数据存放在不同的索引分区
  - 根据数据存放区域进行分区

Schemaless API
====================
首先需要初始化数据库:

      db = Zapian(path='/tmp/test_zapian_db')

添加一个分区：

      db.add_part('2001-02')

添加索引:

      db.add_document(part='2001-02', 
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

搜索：

      db.search(parts, ["and",
                           { "filters":
                              "exclude":
                           },

                           [ "or",
                              {"filters":
                               "exclude": },
                              { "filters":
                                "exclude": }
                           ]
                       ]
                   )


doc和索引的关系
=======================
xapian内部对数据有三种用途：term索引、排序字段、返回data；系统自动对数据类型进行处理：

- set/list/tuple：对每个包含数据，完整匹配搜索(term索引)
- string/unicode: 用于全文搜索(term索引)
- datetime/int/float: 用于排序和范围比较(排序字段)
- 如果字符串类型的字段以 + 开头，表示除了全文索引，也会用于排序

数据库的结构
===================
数据库存放在文件夹结构：

          schema.yaml   # 库结构信息
          20120112/     # 某个分区，标准xapian数据库
          20120512/     # 另外一个分区，标准xapian数据库

其中schema.json, 由系统自动维护, 记录了2个信息：

1. PREFIX和字段的映射： 

      prefixes:{title':"NC", 'created':"LL"}
      
2. attribute存放的slot位置: 

      slots:{'modified':1, 'created':2}

