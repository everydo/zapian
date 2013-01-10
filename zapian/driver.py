# -*- encoding: utf-8 -*-
""" xapian的"分布式"索引引擎

对 internal_doc 进行索引，这个可以放到队列里面在另外的服务器运行 

xapian数据库文件的存放路径::

  DATA_ROOT/
     |
     |-- site_name/
     |     |
     |     |-- catalog_name/
     |     |     |
     |     |     |-- part_name/

问题：

1. 各个site的分区信息维护
2. 删除一个分区

"""

import os
import shutil
import xapian
import json
import cPickle as pickle
from catalog import get_catalog

#DATA_ROOT = ''  # xapian的根文件夹路径
DATA_ROOT = '/home/xtz/hg/edo/var/test_database'  # xapian的根文件夹路径

_qp_flags_base = xapian.QueryParser.FLAG_LOVEHATE
_qp_flags_phrase = xapian.QueryParser.FLAG_PHRASE
_qp_flags_synonym = (xapian.QueryParser.FLAG_AUTO_SYNONYMS |
                     xapian.QueryParser.FLAG_AUTO_MULTIWORD_SYNONYMS)
_qp_flags_bool = xapian.QueryParser.FLAG_BOOLEAN

import logging
logger = logging.getLogger("zapian")

def init_xapian(data_root, **kw):
    global DATA_ROOT
    DATA_ROOT = data_root

def remove_part(site_name, catalog_name, part_name):
    """ 删除一个分区
    """
    if not part_name:
        raise Exception('part_name is emply')

    base_name = os.path.join(DATA_ROOT, site_name, catalog_name, part_name)
    if os.path.isdir(base_name):
        shutil.rmtree(base_name)
        _write_database_index.pop(base_name, None)
    else:
        raise Exception('remove database: %s is not xapian database'%base_name)

def add_document(site_name, catalog_name, part_name, uid, internal_doc, flush=True):
    """ 增加一个索引 """
    db = _get_write_db(site_name, catalog_name, part_name)
    identifier = u'Q' + str(uid)
    doc = _prepare_doc(internal_doc, catalog_name)
    doc.add_boolean_term(identifier)
    db.replace_document(identifier, doc)
    if flush: db.commit()

def replace_document(site_name, catalog_name, part_name,  uid, internal_doc, flush=True):
    """ 重建文档索引 """
    db = _get_write_db(site_name, catalog_name, part_name)
    identifier = u'Q' + str(uid)
    doc = _prepare_doc(internal_doc, catalog_name)
    doc.add_boolean_term(identifier)
    db.replace_document(identifier, doc)
    if flush: db.commit()

def delete_document(site_name, catalog_name, part_name, uids, flush=True):
    """ 删除一个文档 """
    if not isinstance(uids, (list, tuple, set)):
        uids = (uids,)
    db = _get_write_db(site_name, catalog_name, part_name)
    for uid in uids:
        identifier = u'Q' + str(uid)
        db.delete_document(identifier)

    if flush: db.commit()

def update_document(site_name, catalog_name, part_name, uid, internal_doc, flush=True):
    """ 更改文档某个字段的索引

    注意，xapian不支持，这里手工处理 """

    db = _get_write_db(site_name, catalog_name, part_name)
    identifier = u'Q' + str(uid)
    doc = _get_document(db, str(uid))

    new_doc = _prepare_doc(internal_doc, catalog_name, doc)

    db.replace_document(identifier, new_doc)
    if flush: db.commit()

def commit(site_name, catalog_name, part_name):
    """ 对外的接口 """
    db = _get_write_db(site_name, catalog_name, part_name)
    db.commit()

def _get_document(db, uid):
    """Get the document with the specified unique ID.

    Raises a KeyError if there is no such document.  Otherwise, it returns
    a ProcessedDocument.

    """
    postlist = db.postlist('Q' + uid)
    try:
        plitem = postlist.next()
    except StopIteration:
        # Unique ID not found
        raise KeyError('Unique ID %r not found' % uid)
    try:
        postlist.next()
        raise Exception("Multiple documents " #pragma: no cover
                                   "found with same unique ID")
    except StopIteration:
        # Only one instance of the unique ID found, as it should be.
        pass

    return db.get_document(plitem.docid)

_write_database_index = {}
def _get_write_db(site_name, catalog_name, part_name, protocol=''):
    """ 得到可写数据库连结 """
    # FIXME 支持tcp, ssh 协议
    base_name = os.path.join(DATA_ROOT, site_name, catalog_name, part_name)
    # writeable database is already open, this will raise a xapian.DatabaseLockError
    # so, writeable database need to cached.
    if base_name in _write_database_index:
        return _write_database_index[base_name]
    else:
        db = xapian.WritableDatabase(base_name, xapian.DB_CREATE_OR_OPEN)
        _write_database_index[base_name] = db
        return db

def _get_read_db(site_name, catalog_name, parts, protocol=''):
    """ 得到只读数据库连结 """
    # FIXME 支持tcp, ssh 协议
    # 如果没有parts， 默认搜索整个catalog目录下的database
    if not parts:
        parts = []
        catalog_dir = os.path.join(DATA_ROOT, site_name, catalog_name)
        for part in os.listdir(catalog_dir):
            if os.path.isdir(part):
                parts.append(part)

    if not parts: parts = ''

    if not isinstance(parts, (list, tuple, set)):
        parts = (parts,)

    #if not parts:
    #    error_name = os.path.join(DATA_ROOT, site_name, catalog_name)
    #    raise Exception('Do not found the catalog: %s'%error_name)
    
    base_name = os.path.join(DATA_ROOT, site_name, catalog_name, parts[0])
    database = xapian.Database(base_name)

    # 适用于多个数据库查询
    if len(parts) > 1:
        for part_name in parts[1:]:
            other_name = os.path.join(DATA_ROOT, site_name, catalog_name, part_name)
            database.add_database(xapian.Database(other_name))
    return database

def _prepare_doc(internal_doc, catalog_name, xap_doc=None):
    """ 转换internal到xapian的doc 
    """

    doc = xap_doc or xapian.Document()
    termgen = xapian.TermGenerator()
    termgen.set_document(doc)

    # sortable,  旧的value 会被新的value替代
    for field, value in internal_doc['sortable'].iteritems():
        slot_and_type, value = value.items()[0]
        slotnum, types = slot_and_type.split(':')
        if types == 'float':
            value = xapian.sortable_serialise(float(value))
        doc.add_value(int(slotnum), value)

    removed_prefix = set()
    for field, value in internal_doc['fields'].iteritems():
        prefix_and_type, value = value.items()[0]
        prefix, types = prefix_and_type.split(':')

        # 移除旧的term
        if xap_doc and prefix not in removed_prefix:
            termlist = xap_doc.termlist()
            term = termlist.skip_to(prefix)
            while 1:
                if term.term[:2] == prefix:
                    doc.remove_term(term.term)
                else:
                    break
                try:
                    term = termlist.next()
                except StopIteration:
                    break
        removed_prefix.add(prefix)

        if types == 'exact':
            if len(value) > 0:
                # We use the following check, rather than "isupper()" to ensure
                # that we match the check performed by the queryparser, regardless
                # of our locale.
                if ord(value[0]) >= ord('A') and ord(value[0]) <= ord('Z'):
                    prefix = prefix + ':'

            # Note - xapian currently restricts term lengths to about 248
            # characters - except that zero bytes are encoded in two bytes, so
            # in practice a term of length 125 characters could be too long.
            # Xapian will give an error when commit() is called after such
            # documents have been added to the database.
            # As a simple workaround, we give an error here for terms over 220
            # characters, which will catch most occurrences of the error early.
            #
            # In future, it might be good to change to a hashing scheme in this
            # situation (or for terms over, say, 64 characters), where the
            # characters after position 64 are hashed (we obviously need to do this
            # hashing at search time, too).
            if len(prefix + value) > 220:
                raise Exception("Field %r is too long: maximum length "
                                           "220 - was %d (%r)" %
                                           (field, len(prefix + value),
                                            prefix + value))


            doc.add_term(prefix + value, 1) # wdfinc default set 1

        elif types == 'freetext':
            # no positions, weight default set 1
            termgen.index_text_without_positions(value, 1, prefix)
            termgen.increase_termpos(10)

    # store
    data = internal_doc['store']
    if xap_doc:
        old_data = pickle.loads(xap_doc.get_data())
        old_data.update(data)
        doc.set_data(pickle.dumps(old_data))
    else:
        doc.set_data(pickle.dumps(data))

    return doc

def _query_parse_with_prefix(qp, string, flags, prefix):
    """ """
    if prefix is None:
        return qp.parse_query(string, flags)
    else:
        return qp.parse_query(string, flags, prefix)

def _query_parse_with_fallback(qp, string, prefix=None):
    """ """
    try:
        q1 = _query_parse_with_prefix(qp, string,
                                       _qp_flags_base |
                                       _qp_flags_phrase |
                                       _qp_flags_synonym |
                                       _qp_flags_bool,
                                       prefix)
    except xapian.QueryParserError, e:
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
    except xapian.QueryParserError, e:
        # If we got a parse error, retry without boolean operators (since
        # these are the usual cause of the parse error).
        q2 = _query_parse_with_prefix(qp, string,
                                           _qp_flags_base,
                                           prefix)

    return xapian.Query(xapian.Query.OP_AND_MAYBE, q1, q2)

def query_field(field, value, catalog_name, default_op=xapian.Query.OP_AND):
    """ """ 
    catalog = get_catalog(catalog_name)

    # need to check on field type, and stem / split as appropriate

    try:
        types = catalog.fields[field]['type']
    except KeyError:
        types = catalog.attributes[field]['type']

    if types == 'exact':
        prefix = catalog.fields[field]['prefix']
        if len(value) > 0:
            chval = ord(value[0])
            if chval >= ord('A') and chval <= ord('Z'):
                prefix = prefix + ':'
        return xapian.Query(prefix + value)

    if types == 'freetext':
        qp = xapian.QueryParser()
        qp.set_default_op(default_op)
        catalog_field = catalog.fields[field]
        prefix = catalog_field['prefix']
        if 'language' in catalog_field:
            try:
                lang = catalog.fields[field]['language']
                qp.set_stemmer(xapian.Stem(lang))
                qp.set_stemming_strategy(qp.STEM_SOME)
            except KeyError:
                pass
        return _query_parse_with_fallback(qp, value, prefix)

    return xapian.Query()

def query_filter(query, filter, exclude=False):
    """ """
    if not isinstance(filter, xapian.Query):
        raise Exception("filter must be a xapian query object")
    if exclude:
        return xapian.Query(xapian.Query.OP_AND_NOT, query, filter)
    else:
        return xapian.Query(xapian.Query.OP_FILTER, query, filter)

def query_range(field, catalog_name, begin, end):
    """ """
    if begin is None and end is None:
        # Return a "match everything" query
        return xapian.Query('')

    catalog = get_catalog(catalog_name)
    try:
        slot_info = catalog.attributes[field]
    except KeyError:
        # Return a "match nothing" query
        return xapian.Query()

    slot, sorttype = slot_info['slot'], slot_info['type']
    def fn(_value):
        if sorttype == 'float':
            return xapian.sortable_serialise(float(_value))
        return str(_value)

    if begin is not None:
        begin = fn(field, begin)
    if end is not None:
        end = fn(field, end)

    if begin is None:
        return xapian.Query(xapian.Query.OP_VALUE_LE, slot, end)

    if end is None:
        return xapian.Query(xapian.Query.OP_VALUE_GE, slot, begin)

    return xapian.Query(xapian.Query.OP_VALUE_RANGE, slot, begin, end)

def normalize_range(value):
    """ 查询时，转换range 参数，主要是把 float/int 转换为 str 格式 """
    def change_str(item):
        if item is None:
            return item
        else:
            return str(item)
    return [change_str(v) for v in value]

def string2query(query_str, database, catalog_name):
    """ 把json格式的query 转换为xapian 的query
    """
    qp = xapian.QueryParser()
    qp.set_database(database)
    qp.set_default_op(xapian.Query.OP_AND)

    query = json.loads(query_str)
    logger.debug('query string is: %s' % str(query))

    # parse filters
    catalog = get_catalog(catalog_name)
    queries = []
    for filters in query['filters']:
        field, value, op = filters
        if op == 'parse':
            _queries = []
            for f in field:
                prefix = catalog.fields[f]['prefix']
                new_value = value
                # 搜索支持部分匹配

                _queries.append( qp.parse_query(new_value, xapian.QueryParser.FLAG_WILDCARD, prefix) )

            query = xapian.Query(xapian.Query.OP_OR, _queries)

            queries.append(query)
            continue

        if not value:
            continue

        if op == 'allof':
            query = query_field(field, value, catalog_name)
            queries.append(query)

        elif op == 'anyof':
            query = query_field(field, value, catalog_name, default_op=xapian.Query.OP_OR)
            queries.append(query)

        elif op == 'range':
            query = query_range(field, catalog_name, *normalize_range(value))
            queries.append(query)

        elif not op:
            query = query_field(field, value, catalog_name)
            queries.append(query)

    if len(queries) == 1:
        combined = queries[0]
    else:
        _func = lambda q1, q2: query_filter(q1, q2)
        combined = reduce( _func, queries)

    return combined

def search(site_name, catalog_name, parts, query_str, orderby=None, start=0, stop=0):
    """ 搜索, 返回document id的集合 

    如果parts为空，会对此catalog的所有索引进行搜索。
    """
    # 要搜索的数据库位置，允许联合查询
    database = _get_read_db(site_name, catalog_name, parts)

    query = string2query(query_str, database, catalog_name)
    logger.debug("Parsed query is: %s" % str(query))

    enquire = xapian.Enquire(database)
    enquire.set_query(query)

    # sort
    if orderby is not None:
        asc = True
        if orderby[0] == '-':
            asc = False
            orderby = orderby[1:]
        elif orderby[0] == '+':
            orderby = orderby[1:]

        catalog = get_catalog(catalog_name)
        try:
            slotnum = catalog.attributes[orderby]['slot']
        except KeyError:
            raise Exception("Field %r was not indexed for sorting" % orderby)

        # Note: we invert the "asc" parameter, because xapian treats
        # "ascending" as meaning "higher values are better"; in other
        # words, it considers "ascending" to mean return results in
        # descending order.
        enquire.set_sort_by_value_then_relevance(slotnum, not asc)

    enquire.set_docid_order(enquire.DONT_CARE)

    # Repeat the search until we don't get a DatabaseModifiedError
    while True:
        try:
            matches = enquire.get_mset(start, stop)
            break
        except xapian.DatabaseModifiedError:
            database.reopen()

    # 返回结果的ID集
    def _get_docid(match):
        tl = match.document.termlist()
        try:
            term = tl.skip_to('Q').term
            if len(term) == 0 or term[0] != 'Q':
                return None
        except StopIteration:
            return None
        return term[1:]

    return map(lambda match: _get_docid(match), matches)

