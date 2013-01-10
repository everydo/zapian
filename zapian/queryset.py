# -*- encoding: utf-8 -*-

"""
参考： www.elasticsearch.org/
"""

import types, datetime
import json
import utils

class CombinedQuerySet:

    def __init__(self, op, query1, query2):
        self.data = []
        self.data.append(op)
        self.data.append(query1.data())
        self.data.append(query2.data())

    def data(self):
        return self.data

    def json(self):
        return json.dumps(self.data)

class QuerySet(object):
    """ 一组任何的查询条件组合 

    查询条件：all in equal range parse or
    结果：sort，limit
['and',

  {},
   
    ["and",

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

    """

    def __init__(self,  start=0, limit=0, attr_gen=None, attr_clean=None, **kwargs):
        """ 初始化 """
        self._filters = []
        self._exclude = []
        self._order_by = None

        # 查询结果的范围限制
        self._limit = limit
        self._start = start
        self._attr_clean = attr_clean if not attr_clean is None else utils.clean_field

        # 多余的都是过滤条件
        for key,value in kwargs.items():
            self.filter(**{key:value})

    def data(self):
        data = {}
        data['filters'] = self._filters
        data['exclude'] = self._exclude
        return data

    def json(self):
        """ """
        return json.dumps(self.data())

    def limit(self, limit):
        """ 取limit个结果 """
        if limit:
            self._limit = limit
        return self

    def sort(self, field):
        """ 按field进行排序
            - field 要排序的索引字段，可已"+" 或"-" , 以"-"开头时倒序排列
            注意: 调用此函数后并不执行查询
            一个QuerySet 只能有一个排序字段
        """
        if self._order_by is not None:
            raise ValueError, "there must be only one sort field"

        self._order_by = field
        return self

    def exclude(self, **expression):
        """ 排除条件符合条件的结果 """
        return self.filter(exclude=True, **expression)

    def filter(self, exclude=False, **expression):
        """ 添加条件
            格式: field_name__ops=value

            - field_nam 索引字段

            - ops

            支持的操作符：

            - range   进行range查询(只有数字、日期字段才可进行range查询)
            - allof
            - anyof
        """
        args_len = len(expression.items())
        if args_len != 1:
            raise TypeError, "filter takes 2 arguments (%d given)" % (args_len + 1)

        for field, value in expression.items():

            # 使用了__后缀查询
            if len(field.split('__')) > 1:
                if not len(field.split('__')) == 2:
                    raise TypeError, "Unsupported filter expression"

                field, op = field.split('__')

                if op == 'allof':
                    # allof 查询
                    if not type(value) in (tuple, list):
                        raise TypeError, "__allof filter's value must be a list "

                    filter = (field, value, 'allof')

                elif op == 'anyof':
                    # anyof 查询
                    if not type(value) in (tuple, list):
                        raise TypeError, "__anyof filter's value must be a list "
 
                    filter = (field, value, 'anyof')

                elif op == 'range':
                    # 查询区间
                    if not type(value) in (list, tuple):
                        raise TypeError, "range paramter must be list or tuple"

                    if not len(value) == 2:
                        raise TypeError, "range paramter's length must be 2"

                    if not type(value[0]) in (int, float, types.NoneType, datetime) and type(value[1]) in (int, float, types.NoneType, datetime):
                        raise TypeError, "range paramter's item must be int or float or None"

                    filter = (field, value, 'range')

                else:
                    raise SyntaxError, "unsupported filter op"

            # 没有使用 __ 后缀
            else:
                # 如果filter 的参数是个list则默认使用allof查询
                if type(value) in (list, tuple):
                    self._filters.append((field, value, 'allof'))

                filter = (field, value, '')

            field, value, op = filter
            value = self._attr_clean(field, value, is_query=True)
            if type(value) in (int, float, long):
                value = str(value)

            if exclude:
                self._exclude.append((field, value, op))
            else:
                self._filters.append((field, value, op))

        return self
 
    def parse(self, text, fields):
        """ 跨字段搜索函数

            - fields 允许搜索的字段，为None则搜索全部字段
        """

        self._filters.append((fields, text, 'parse'))
        return self

    def __or__(self, other):
        """
           逻辑或重载，用来将两个QuerySet 用OR合并
           **第二个QuerySet 的某些参数将被忽略(start, limit, sort, unique)**
           RETURN一个新的QuerySet 对象
        """
        return CombinedQuerySet('or', self, other)

    def __and__(self, other):
        """
           逻辑或重载，用来将两个QuerySet 用OR合并
           **第二个QuerySet 的某些参数将被忽略(start, limit, sort, unique)**
           RETURN一个新的QuerySet 对象
        """
        return CombinedQuerySet('and', self, other)

