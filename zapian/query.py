# -*- coding: utf-8 -*- 

import xapian
from utils import clean_value, clean_date

_qp_flags_base = xapian.QueryParser.FLAG_LOVEHATE
_qp_flags_phrase = xapian.QueryParser.FLAG_PHRASE
_qp_flags_synonym = (xapian.QueryParser.FLAG_AUTO_SYNONYMS |
                     xapian.QueryParser.FLAG_AUTO_MULTIWORD_SYNONYMS)
_qp_flags_bool = xapian.QueryParser.FLAG_BOOLEAN


class Query(object):

    """ query class """

    def __init__(self, schema):
        """ 初始化 """
        self.schema = schema
        self._filters = []
        self._exclude = []
        self._combine_operation = 'and'
        self._combine_query = None

    def _combine(self, other, operation):
        if not isinstance(other, type(self)):
            raise TypeError, "the other query must be Query instance"

        if self._combine_query is not None:
            self._combine_query._combine(other, operation)
        else:
            self._combine_operation = operation
            self._combine_query = other
            return self

    def __or__(self, other):
        """
           逻辑或重载，用来将两个QuerySet 用OR合并
           **第二个QuerySet 的某些参数将被忽略(start, limit, restricted, sort, unique)**
           RETURN一个新的QuerySet 对象
        """
        return self._combine(other, 'or')

    def __and__(self, other):
        """
           逻辑或重载，用来将两个QuerySet 用and合并
           **第二个QuerySet 的某些参数将被忽略(start, limit, restricted, sort, unique)**
           RETURN一个新的QuerySet 对象
        """
        return self._combine(other, 'and')

    def exclude(self, key, value, op):
        """ 排除条件符合条件的结果 """
        return self.filter(key, value, op, exclude=True)

    def parse(self, text, fields):
        """ 跨字段搜索函数, 可以使用通配符
            - fields 允许搜索的字段，为None则搜索全部字段
        """
        # 搜索支持部分匹配
        self._filters.append((fields, text, 'parse'))
        return self

    def filter(self, key, value, op, exclude=False):
        """ 添加条件

            - ops
            支持的操作符：

            - range   进行range查询(只有数字、日期字段才可进行range查询)
            - allof
            - anyof
        """
        if exclude:
            self._exclude.append((key, value, op))
        else:
            self._filters.append((key, value, op))

        return self

    def _get_xapian_query(self, querys, exclude=False, database=None):
        """ convert to xapian query 
        combined = query_filter(self.query_restricted(), combined)
        """
        if not querys:
            return xapian.Query('')

        qp = xapian.QueryParser()
        if database is not None:
            qp.set_database(database)
        qp.set_default_op(xapian.Query.OP_AND)

        # parse filters
        queries = []
        for filters in querys:
            field, value, op = filters
            if op == 'parse':
                _queries = []
                value = clean_value(value, is_query=True)
                for f in field:
                    prefix = self.schema.get_prefix(f, auto_add=False)
                    _queries.append( qp.parse_query(value, xapian.QueryParser.FLAG_WILDCARD, prefix) )

                query = xapian.Query(xapian.Query.OP_OR, _queries)

                queries.append(query)
                continue

            if not value:
                continue

            if op == 'allof':
                prefix = self.schema.get_prefix(field, auto_add=False)
                value = clean_value(value)
                query = query_field(prefix, value)
                queries.append(query)

            elif op == 'anyof':
                prefix = self.schema.get_prefix(field, auto_add=False)
                value = clean_value(value)
                query = query_field(prefix, value, default_op=xapian.Query.OP_OR)
                queries.append(query)

            elif op == 'range':
                prefix = self.schema.get_slot(field, auto_add=False)
                value = clean_date(value)
                begin, end = value[:2]
                query = self.query_range(prefix, begin, end)
                queries.append(query)

            elif not op:
                prefix = self.schema.get_prefix(field, auto_add=False)
                value = clean_value(value)
                query = query_field(prefix, value)
                queries.append(query)

        if len(queries) == 1:
            combined = queries[0]
        else:
            if not exclude:
                _func = lambda q1, q2: query_filter(q1, q2)
                combined = reduce( _func, queries)
            else:
                combined = xapian.Query(xapian.Query.OP_OR, list(queries))

        return combined

    def query_range(self, slot, begin, end):
        """ """
        if begin is None and end is None:
            # Return a "match everything" query
            return xapian.Query('')

        if slot is None:
            # Return a "match nothing" query
            return xapian.Query()

        begin, end = normalize_range(begin, end)

        if begin is None:
            return xapian.Query(xapian.Query.OP_VALUE_LE, slot, end)

        if end is None:
            return xapian.Query(xapian.Query.OP_VALUE_GE, slot, begin)

        return xapian.Query(xapian.Query.OP_VALUE_RANGE, slot, begin, end)

    def unique(self, field):
        """ 对指定字段，如有多个相同的值则返回结果中只出现一次

            **只有设置了collapse 的字段才可进行unique查询**
        """
        self._collapse = field
        return self

    def to_dict(self):
        """  将query对象转换为基本的python字典对象
        """
        # TODO

    def build_query(self, database=None):
        """ 生成xappy查询
            RETURN Xapian Query 对象

            database: xapian 使用通配符模式搜索，需要指定数据库对象
                      如果你使用了parse方法，需要在这里指定数据库对象
        """
        source = self
        other = self._combine_query
        operation = self._combine_operation

        # 先对自己build query
        combined = source._get_xapian_query(source._filters, database=database)
        # 添加exclude的查询条件
        if source._exclude:
            exclude_query = source._get_xapian_query(source._exclude, True, database=database)
            if exclude_query:
                combined = query_filter(combined, exclude_query, exclude=True)

        # 递归合并多个Query, 只允许and, or两种操作
        while other is not None:
            combined2 = other._get_xapian_query(other._filters, database=database)
            # 添加exclude的查询条件
            if self._exclude:
                exclude_query = other._get_xapian_query(other._exclude, True, database=database)
                if exclude_query:
                    combined2 = query_filter(combined2, exclude_query, exclude=True)

            if operation == 'and':
                combined = query_filter(combined, combined2)
            else:
                combined = xapian.Query(xapian.Query.OP_OR, [combined, combined2])

            operation = other._combine_operation
            other = other._combine_query

        return combined

def normalize_range(begin, end):
    """ 查询时，转换range 参数，主要是把 float/int 转换为 str 格式 """

    if begin is not None:
        if isinstance(begin, float):
            begin = xapian.sortable_serialise(float(begin))
        else:
            begin = str(begin)

    if end is not None:
        if isinstance(end, float):
            end = xapian.sortable_serialise(float(end))
        else:
            end = str(end)
    return begin, end

def _query_parse_with_prefix(qp, string, flags, prefix):
    """ """
    if prefix is None:
        return qp.parse_query(string, flags)
    else:
        return qp.parse_query(string, flags, prefix)

def query_filter(query, filter, exclude=False):
    """ """
    if not isinstance(filter, xapian.Query):
        raise Exception("filter must be a xapian query object")
    if exclude:
        return xapian.Query(xapian.Query.OP_AND_NOT, query, filter)
    else:
        return xapian.Query(xapian.Query.OP_FILTER, query, filter)

def query_field(prefix, value, default_op=xapian.Query.OP_AND):
    """ """ 
    #if types == 'exact':
    #    prefix = catalog.fields[field]['prefix']
    #    if len(value) > 0:
    #        chval = ord(value[0])
    #        if chval >= ord('A') and chval <= ord('Z'):
    #            prefix = prefix + ':'
    #    return xapian.Query(prefix + value)

    #if types == 'freetext':
    #    qp = xapian.QueryParser()
    #    qp.set_default_op(default_op)
    #    prefix = self.get_prefix(field)
    #    return _query_parse_with_fallback(qp, value, prefix)

    #return xapian.Query()


    if not prefix:
        return xapian.Query()
    else:
        qp = xapian.QueryParser()
        qp.set_default_op(default_op)
        return _query_parse_with_fallback(qp, value, prefix)

def _query_parse_with_fallback(qp, string, prefix=None):
    """ """
    try:
        q1 = _query_parse_with_prefix(qp, string,
                                       _qp_flags_base |
                                       _qp_flags_phrase |
                                       _qp_flags_synonym |
                                       _qp_flags_bool,
                                       prefix)
    except xapian.QueryParserError:
        # If we got a parse error, retry without boolean operators (since
        # these are the usual cause of the parse error).
        q1 = _query_parse_with_prefix(qp, string,
                                           _qp_flags_base |
                                           _qp_flags_phrase |
                                           _qp_flags_synonym,
                                           prefix)

    qp.set_stemming_strategy(qp.STEM_NONE)
    try:
        q2 = _query_parse_with_prefix(qp, string,
                                           _qp_flags_base |
                                           _qp_flags_bool,
                                           prefix)
    except xapian.QueryParserError:
        # If we got a parse error, retry without boolean operators (since
        # these are the usual cause of the parse error).
        q2 = _query_parse_with_prefix(qp, string,
                                           _qp_flags_base,
                                           prefix)

    return xapian.Query(xapian.Query.OP_AND_MAYBE, q1, q2)

