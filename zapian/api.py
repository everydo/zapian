# -*- encoding: utf-8 -*-
""" zapian api 
"""

import os
import gc
import shutil
import xapian
import time
import hashlib
import cPickle as pickle
from threading import local
from datetime import datetime

from query import Query
from utils import clean_value
from schema import Schema

class Zapian(object):

    def __init__(self, db_path):
        self.db_path = db_path
        self.schema = Schema(db_path)

    @property
    def parts(self):
        """ show parts of the database """
        for sub in os.listdir(self.db_path):
            if os.path.isdir(os.path.join(self.db_path, sub)):
                yield sub

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

    def get_interior_doc(self, doc, data=None, old_doc=None):
        """ convert python dict into xapian document object
            doc: 
               {field1:value1, 
                field2:value2}

            data: raw data for original object

            return: xapian document object
        """
        def _add_term(doc, termgen, prefix, value):
            type_name = 'freetext'

            if type_name == 'exact':
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
                    raise Exception("Field is too long: maximum length "
                                               "220 - was %d (%r)" %
                                               (len(prefix + value),
                                                prefix + value))


                doc.add_term(prefix + value, 1) # wdfinc default set 1

            elif type_name == 'freetext':
                # no positions, weight default set 1
                termgen.index_text_without_positions(str(value), 1, prefix)
                termgen.increase_termpos(10)

        def _add_value(doc, slotnum, value):

            if isinstance(value, float):
                value = xapian.sortable_serialise(float(value))
                doc.add_value(int(slotnum), value)
            else:
                doc.add_value(int(slotnum), str(value))

        document = xapian.Document()
        termgen = xapian.TermGenerator()
        termgen.set_document(document)

        terms = set()
        values = set()
        # build new xapian object
        for field, value in doc.iteritems():
            both = field.startswith('+') 
            if both:
                field = field[1:]
            is_value = isinstance(value, (int, float, datetime))
            is_term = not is_value
            # sortable
            if both or is_value:
                if field in values:
                    continue
                slotnum = self.schema.get_slot(field)
                value = clean_value(value)
                _add_value(document, slotnum, value)
                values.add(slotnum)
            # field
            if both or is_term:
                if field in terms:
                    continue
                prefix = self.schema.get_prefix(field)
                value = clean_value(value)
                _add_term(document, termgen, prefix, value)
                terms.add(prefix)

        # new value will be replace old value
        if old_doc is not None:
            for term in old_doc.termlist():
                prefix, value = self.schema.split_term(term.term)
                if prefix not in terms:
                    _add_term(document, termgen, prefix, value)

            for value in old_doc.values():
                if value.num not in values:
                    _add_value(document, value.num, value.value)

            if data is None: 
                data = dict()

            old_data = old_doc.get_data()
            if old_data:
                old_data = pickle.loads(old_data)
                for k, v in old_data.iteritems():
                    if k not in data:
                        data[k] = v
        # add data
        if data:
            document.set_data(pickle.dumps(data))

        return document

    def add_document(self, part_name, index, uid=None, data=None, flush=True):
        """ add a xapian document
        """
        db = _get_write_db(self.db_path, part_name)
        doc = self.get_interior_doc(index, data=data)
        if not uid:
            identifier = u'Q' + part_name + str(db.get_lastdocid())
        else:
            identifier = u'Q' + str(uid)
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
        old_doc = self._get_document(str(uid), [part_name], db=db)

        new_doc = self.get_interior_doc(index, data=data, old_doc=old_doc)
        
        db.replace_document(identifier, new_doc)
        if flush: db.commit()

    def get_document(self, uid, part=None):
        """ Get the document """
        xap_doc = self._get_document(uid, part)
        return pickle.loads( xap_doc.get_data() )

    def _get_document(self, uid, part=None, db=None):
        """Get the xapian document object with the specified unique ID.

        Raises a KeyError if there is no such document.  Otherwise, it returns
        a ProcessedDocument.

        """
        if db is None:
            if part is None:
                raise Exception('_get_document method need the part or the db')
            else:
                db = _get_read_db(self.db_path, part)

        while True:
            try:
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

            except xapian.DatabaseModifiedError:
                reopen(db)

    def commit(self, parts=None):
        """ commit xapian database """
        if parts is None:
            parts = ['default']

        for part_name in parts:
            db = _get_write_db(self.db_path, part_name)
            db.commit()

    def search(self, parts=None, query=None, query_obj=None, orderby=None, start=None, stop=None):
        """ 搜索, 返回document id的集合 

        如果parts为空，会对此catalog的所有索引进行搜索。

        """
        # 这个目录不一个正确的数据库，可能还没有保存至少一条数据
        #if CONFIG_FILE not in os.listdir(self.db_path):
        #    return []
        if parts is None:
            parts = ['default']

        if not parts:
            return []

        database = _get_read_db(self.db_path, parts=parts)

        if query_obj is not None:
            xapian_query = query_obj.build_query(database=database)
        else:
            query_obj = Query(self.schema)
            query_obj._filters = query
            xapian_query = query_obj.build_query(database=database)

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

            slotnum = self.schema.get_slot(orderby, auto_add=False)
            # sort when available
            if slotnum is not None:
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
                reopen(database)

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

thread_context = local()
READ_DB_REFRESH_DELTA = 2 # max time in seconds till we refresh a connection
def _get_read_db(db_path, parts, protocol=''):
    """ get xapian readonly database
        protocol: the future maybe support.
    """
    if not getattr(thread_context, 'connection', None):
        thread_context.connection = {}

    if not getattr(thread_context, 'modified', None):
        thread_context.modified = {}

    if not getattr(thread_context, 'opened', None):
        thread_context.opened = {}

    prefix = hashlib.md5(db_path + ''.join(parts)).hexdigest()
    conn = thread_context.connection.get(prefix, None)
    now = time.time()

    if thread_context.modified.get(prefix, 0) + READ_DB_REFRESH_DELTA < now:
        thread_context.modified[prefix] = now
    
    if conn is None:
        # 适用于多个数据库查询
        for path_name in parts:
            path = os.path.join(db_path, path_name)
            try:
                db = xapian.Database(path)
            except xapian.DatabaseOpeningError:
                continue

            if conn is None:
                conn = db
            else:
                conn.add_database(db)

        thread_context.connection[prefix] = conn 
        thread_context.opened[prefix] = now
            
    opened = thread_context.opened.get(prefix, 0)

    if opened < thread_context.modified[prefix]:
        reopen(conn)
        thread_context.opened[prefix] = now

    return conn

def _release_read_db(db_path, parts, protocol=''):
    """ release the read db 
        protocol: the future maybe support.
    """
    prefix = hashlib.md5(db_path + ''.join(parts)).hexdigest()
    del thread_context.modified[prefix]
    del thread_context.opened[prefix]
    thread_context.connection[prefix].close()
    del thread_context.connection[prefix]

def reopen(db):
    db.reopen()
    gc.collect()
