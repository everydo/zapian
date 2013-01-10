# -*- encoding: utf-8 -*-
from queryset import QuerySet
from xapian_driver import query_field
from utils import clean_field
import catalog
import engine

class LockException(Exception):
    """ 索引数据库锁错误 """

