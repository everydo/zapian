# -*- encoding: utf-8 -*-

""" catalog管理

"""
from catalog import get_catalog
from time import mktime
from types import StringTypes
from cjksplitter import CJKSplitter

def process_doc(catalog_name, raw_doc):
    """ 将原始的doc，根据catalog信息，转换为内部的doc形式，用于存放到队列

       raw_doc: 原始的doc 

           {field1:value1, 
            field2:value2}

       返回: 

           {'fields':{"prefix:type":value}, 
            'attributes':{"slot:type":12}, 
            'data':{}}
    """
    internal_doc = {'sortable':{}, 'fields':{}, 'store':{}}
    catalog = get_catalog(catalog_name)
    for field, value in raw_doc.iteritems():

        value = CLEANERS[type(value)](value, False)

        # 一个field 可能同时有3种角色
        if field in catalog.fields:
            catalog_fields = catalog.fields[field]
            prefix, types = catalog_fields['prefix'], catalog_fields['type']
            internal_doc['fields'][field] = {"%s:%s"%(prefix, types): value}

        if field in catalog.attributes:
            catalog_fields = catalog.attributes[field]
            slot, types = catalog_fields['slot'], catalog_fields['type']
            internal_doc['sortable'][field] = {"%s:%s"%(slot, types): value}

        if field in catalog.data:
            try:
                field_data = internal_doc['store'][field]
            except KeyError:
                field_data = []
                internal_doc['store'][field] = field_data
            field_data.append( value )

    return internal_doc

def datetimeNorm(value, is_query=False):
    value = int(mktime(value.timetuple()))
    return value

def clean_splitter(value, is_query):
    """ 分词 """
    splitter = CJKSplitter()
    if is_query:
        return " ".join(splitter.process([value], 1))
    else:
        return " ".join(splitter.process([value]))

def clean_date(value, is_query):
    """ 对时间进行清理 """
    if type(value) in (tuple, list):
        if value[0] is None and value[1] is None:
            return 
        elif value[0] is None:
            return None, datetimeNorm(value[1])
        elif value[1] is None:
            return datetimeNorm(value[0]), None
        else:
            return datetimeNorm(value[0]), datetimeNorm(value[1])
    if value in ['', u'', None]: return None
    return datetimeNorm(value)

def clean_list(value, is_query):
    """ 将一个包含字符串的list 转换为字符串，字符串中的“.”"@"和空格将被转换为_"""
    _rep = lambda s : s.replace(" ", "_").replace(".", "_").replace('@', '_').replace("-", '_')
    if type(value) not in StringTypes:
        value =  [ _rep(s) for s in value ] 
        return " ".join(value)
    else:
        return _rep(value)

def clean_parent(obj, is_query):
    """ 得到一个folder的父容器ID """
    if type(obj) == int:
        # 用于搜索
        return str(obj)
    else:
        return  str(pathNorm(obj, True)[0])

def clean_path(objs, is_query):
    """ 转换obj 为一个整数path

    RETURN      由父对象ID组成的字符串 
    """
    value = []

    if type(objs) in (tuple, list):
        # 正在查询时调用
        for obj in objs:
            if type(obj) == int:
                value.append(obj)
            else:
                value.extend(pathNorm(obj, True))
    else:
        # 建立索引时调用
        value.extend(pathNorm(objs, False))

    if not value:
        return ""

    return " ".join([ str(id) for id in value ])

# 这个字段指定了所要特殊处理格式的的字段和处理函数
CLEANERS = {
    unicode:                  clean_splitter,
    str:                      clean_splitter,
    datetime:                 clean_date,
    date:                     clean_date,
    list:                     clean_list,
    set:                      clean_list,
}

