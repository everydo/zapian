# -*- encoding: utf-8 -*-

import re
from datetime import datetime
from time import mktime
from cjksplitter import CJKSplitter

LIST_TYPES = (set, list, tuple)
STRING_TYPES = (str, unicode)
DATE_TYPES = (datetime, int, float, long)

def clean_value(value, is_query=False):
    """ convert value 
    """
    if isinstance(value, LIST_TYPES):
        return clean_list(value)
    elif isinstance(value, STRING_TYPES):
        return clean_splitter(value, is_query=is_query)
    elif isinstance(value, DATE_TYPES):
        return clean_date(value)
    elif value is None:
        return 'None'
    else:
        raise TypeError('%s object is not supported type. value is %s .' % (type(value), str(value))  )

def datetimeNorm(value, is_query=False):
    if isinstance(value, datetime):
        return int(mktime(value.timetuple()))
    return value

def clean_splitter(value, is_query=False):
    """ 分词 """
    splitter = CJKSplitter()
    if is_query:
        return " ".join(splitter.process([value], 1))
    else:
        return " ".join(splitter.process([value]))

def clean_date(value):
    """ 对时间进行清理 """
    if type(value) in LIST_TYPES:
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

CLEAN_LIST_RE = re.compile(r'[\s.@-]')
def clean_list(value):
    """ 将一个包含字符串的list 转换为字符串，字符串中的“.”"@"和空格将被转换为_"""
    if type(value) not in STRING_TYPES:
        value =  [ CLEAN_LIST_RE.sub('_', str(s)) for s in value ] 
        return " ".join(value)
    else:
        return CLEAN_LIST_RE.sub('_', str(value))

