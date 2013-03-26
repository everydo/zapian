# -*- encoding: utf-8 -*-
""" zapian api 
"""

import os
import shutil
import xapian
import cPickle as pickle
from datetime import datetime

from utils import clean_value
from schema import Schema

_qp_flags_base = xapian.QueryParser.FLAG_LOVEHATE
_qp_flags_phrase = xapian.QueryParser.FLAG_PHRASE
_qp_flags_synonym = (xapian.QueryParser.FLAG_AUTO_SYNONYMS |
                     xapian.QueryParser.FLAG_AUTO_MULTIWORD_SYNONYMS)
_qp_flags_bool = xapian.QueryParser.FLAG_BOOLEAN

class Zapian(Schema):

    def __init__(self, db_path):
        self.db_path = db_path
        self.parts = [] 
        super(self.__class__, self).__init__(db_path)

        for part_name in os.listdir(db_path):
            self.add_part(part_name)

    def add_part(self, part_name):
        """ add xapian a database """
        part_path = os.path.join(self.db_path, part_name)
        if os.path.isdir(part_path):
            self.parts.append(part_name)

    def release_part(self, part_name):
        """ release xapian a database """
        _release_write_db(self.db_path, part_name)

    def remove_part(self, part_name):
        """ remove xapian a database """
        part_path = os.path.join(self.db_path, part_name)
        if os.path.isdir(part_path):
            part = _write_database_index.pop(part_path, None)
            part.close()
            shutil.rmtree(part_path)
        else:
            raise Exception('remove database: %s is not xapian database'%part_path)

    def get_interior_doc(self, doc, data=None, xap_doc=None):
        """ convert python dict into xapian document object
            doc: 
               {field1:value1, 
                field2:value2}

            data: raw data for original object

            return: xapian document object
        """
        document = xap_doc or xapian.Document()
        termgen = xapian.TermGenerator()
        termgen.set_document(document)

        removed_prefix = set()
        # new value will be replace old value
        for field, value in doc.iteritems():

            # sortable
            if isinstance(value, (int, float, datetime)):
                value = clean_value(value)

                slotnum = self.get_slot(field)
                if isinstance(value, float):
                    value = xapian.sortable_serialise(float(value))
                    document.add_value(int(slotnum), value)
                else:
                    document.add_value(int(slotnum), str(value))

            # field
            else:
                value = clean_value(value)
                prefix = self.get_prefix(field)
                types = 'freetext'

                # 移除旧的term
                if xap_doc and prefix not in removed_prefix:
                    termlist = xap_doc.termlist()

                    try:
                        term = termlist.skip_to(prefix)
                        has_old_term = True
                    except:
                        has_old_term = False

                    while has_old_term:
                        if term.term.startswith(prefix):
                            document.remove_term(term.term)
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


                    document.add_term(prefix + value, 1) # wdfinc default set 1

                elif types == 'freetext':
                    # no positions, weight default set 1
                    termgen.index_text_without_positions(str(value), 1, prefix)
                    termgen.increase_termpos(10)

        # data
        if data is not None:
            if xap_doc:
                old_data = pickle.loads(xap_doc.get_data())
                old_data.update(data)
                document.set_data(pickle.dumps(old_data))
            else:
                document.set_data(pickle.dumps(data))

        return document

    def add_document(self, part_name, uid, index, data=None, flush=True):
        """ add a xapian document
        """
        doc = self.get_interior_doc(index, data=data)
        identifier = u'Q' + str(uid)
        db = _get_write_db(self.db_path, part_name)
        doc.add_boolean_term(identifier)
        db.replace_document(identifier, doc)
        if flush: db.commit()

    def replace_document(self, part_name, uid, index, data=None, flush=True):
        """ replace existing xapian document """
        doc = self.get_interior_doc(index, data=data)
        db = _get_write_db(self.db_path, part_name)
        identifier = u'Q' + str(uid)
        doc.add_boolean_term(identifier)
        db.replace_document(identifier, doc)
        if flush: db.commit()

    def delete_document(self, part_name, uids, flush=True):
        """ delete a xapian document """
        if not isinstance(uids, (list, tuple, set)):
            uids = (uids,)
        db = _get_write_db(self.db_path, part_name)
        for uid in uids:
            identifier = u'Q' + str(uid)
            db.delete_document(identifier)
        if flush: db.commit()

    def update_document(self, part_name, uid, index, data=None, flush=True):
        """ update xapian document existing fields and attributes
        """
        db = _get_write_db(self.db_path, part_name)

        identifier = u'Q' + str(uid)
        old_doc = self._get_document(str(uid), [part_name])

        new_doc = self.get_interior_doc(index, data=data, xap_doc=old_doc)
        
        db.replace_document(identifier, new_doc)
        if flush: db.commit()

    def get_document(self, uid, part=None):
        """ Get the document """
        xap_doc = self._get_document(uid, part)
        return pickle.loads( xap_doc.get_data() )

    def _get_document(self, uid, part=None):
        """Get the xapian document object with the specified unique ID.

        Raises a KeyError if there is no such document.  Otherwise, it returns
        a ProcessedDocument.

        """
        db = _get_read_db(self.db_path, part or self.parts)
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

    def commit(self, part_name):
        """ commit xapian database """
        db = _get_write_db(self.db_path, part_name)
        db.commit()

    def _get_xapian_query(self, querys, database=None, db_path=None, parts=None):
        """ convert to xapian query 
        """
        if database is None:
            database = _get_read_db(db_path, parts=parts or self.parts)

        qp = xapian.QueryParser()
        qp.set_database(database)
        qp.set_default_op(xapian.Query.OP_AND)

        # parse filters
        if not querys:
            return xapian.Query('')

        queries = []
        for filters in querys:
            field, value, op = filters
            if op == 'parse':
                _queries = []
                for f in field:
                    prefix = self.get_prefix(f, auto_add=False)
                    # 搜索支持部分匹配
                    _queries.append( qp.parse_query(value, xapian.QueryParser.FLAG_WILDCARD, prefix) )

                query = xapian.Query(xapian.Query.OP_OR, _queries)

                queries.append(query)
                continue

            if not value:
                continue

            if op == 'allof':
                query = self.query_field(field, value)
                queries.append(query)

            elif op == 'anyof':
                query = self.query_field(field, value, default_op=xapian.Query.OP_OR)
                queries.append(query)

            elif op == 'range':
                begin, end = value[:2]
                query = self.query_range(field, begin, end)
                queries.append(query)

            elif not op:
                query = self.query_field(field, value)
                queries.append(query)

        if len(queries) == 1:
            combined = queries[0]
        else:
            _func = lambda q1, q2: query_filter(q1, q2)
            combined = reduce( _func, queries)

        return combined

    def search(self, parts=None, query=None, orderby=None, start=None, stop=None):
        """ 搜索, 返回document id的集合 

        如果parts为空，会对此catalog的所有索引进行搜索。
        如果query为空，默认返回全部结果
        """
        if parts is None:
            parts = self.parts

        for part_name in parts:
            if not os.path.exists(os.path.join(self.db_path, part_name)):
                parts.remove(part_name)

        if not parts:
            return []

        database = _get_read_db(self.db_path, parts=parts or self.parts)
        xapian_query = self._get_xapian_query(query, database=database)
        enquire = xapian.Enquire(database)
        enquire.set_query(xapian_query)
        
        # sort
        if orderby is not None:
            asc = True
            if orderby[0] == '-':
                asc = False
                orderby = orderby[1:]
            elif orderby[0] == '+':
                orderby = orderby[1:]

            slotnum = self.get_slot(orderby, auto_add=False)
            if slotnum is None:
                raise Exception("Field %r was not indexed for sorting" % orderby)

            # Note: we invert the "asc" parameter, because xapian treats
            # "ascending" as meaning "higher values are better"; in other
            # words, it considers "ascending" to mean return results in
            # descending order.
            enquire.set_sort_by_value_then_relevance(slotnum, not asc)

        enquire.set_docid_order(enquire.DONT_CARE)

        # Repeat the search until we don't get a DatabaseModifiedError
        if start is None: start = 0
        if stop is None: stop = database.get_doccount()
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

        return map(_get_docid, matches)

    def query_field(self, field, value, default_op=xapian.Query.OP_AND):
        """ """ 
        #try:
        #    types = catalog.fields[field]['type']
        #except KeyError:
        #    types = catalog.attributes[field]['type']

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

        # FIXME
        prefix = self.get_prefix(field, auto_add=False)

        if not prefix:
            return xapian.Query()
        else:
            qp = xapian.QueryParser()
            qp.set_default_op(default_op)
            return _query_parse_with_fallback(qp, value, prefix)

    def query_range(self, field, begin, end):
        """ """
        if begin is None and end is None:
            # Return a "match everything" query
            return xapian.Query('')

        slot = self.get_slot(field, auto_add=False)
        if slot is None:
            # Return a "match nothing" query
            return xapian.Query()

        begin, end = normalize_range(begin, end)

        if begin is None:
            return xapian.Query(xapian.Query.OP_VALUE_LE, slot, end)

        if end is None:
            return xapian.Query(xapian.Query.OP_VALUE_GE, slot, begin)

        return xapian.Query(xapian.Query.OP_VALUE_RANGE, slot, begin, end)

_write_database_index = {}
def _get_write_db(db_path, part_name, protocol=''):
    """ get xapian writable database 
        protocol: the future may support.
    """
    part_path = os.path.join(db_path, part_name)
    # writeable database is already open, this will raise a xapian.DatabaseLockError
    # so, writeable database need to cached.
    if part_path in _write_database_index:
        return _write_database_index[part_path]
    else:
        db = xapian.WritableDatabase(part_path, xapian.DB_CREATE_OR_OPEN)
        _write_database_index[part_path] = db
        return db

def _release_write_db(db_path, part_name, protocol=''):
    """ """
    part_path = os.path.join(db_path, part_name)
    if part_path in _write_database_index:
        _write_database_index[part_path].close()
        del _write_database_index[part_path]

def _get_read_db(db_path, parts, protocol=''):
    """ get xapian readonly database
        protocol: the future maybe support.
    """
    base_name = os.path.join(db_path, parts[0])
    database = xapian.Database(base_name)

    # 适用于多个数据库查询
    for part_name in parts[1:]:
        other_name = os.path.join(db_path, part_name)
        database.add_database(xapian.Database(other_name))

    return database

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

def query_filter(query, filter, exclude=False):
    """ """
    if not isinstance(filter, xapian.Query):
        raise Exception("filter must be a xapian query object")
    if exclude:
        return xapian.Query(xapian.Query.OP_AND_NOT, query, filter)
    else:
        return xapian.Query(xapian.Query.OP_FILTER, query, filter)

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
