# -*- coding: utf-8 -*-
"""
USAGE:

python xappy.py --<list|count|help>  < -i database > [ -n numbers, -s start ]

-l, --list      打印所有数据
可选参数: 
    -s --start   从第几条记录开始
    -n --numbers 打印多少条记录

-c, --count     列出记录数
-f, --fields    打印指定数据库中所有索引字段的名字

EXAMPLE:
    -c -i xapian.idx
    -l -i xapian.idx
    -l -i xapian.idx -s 0 -n 100
    -f -i xapian.idx
"""

import sys
import getopt
import xappy

def usage():
    print __doc__ 

def print_document_count(conn):
    """ 得到某个数据库中所有记录的个数 """
    print "共有%10d 条记录" % conn.get_doccount()

def print_documents(conn, start, limit):
    """ 打印数据库中的记录 """
    query = conn.query_all()
    if limit == 0:
        limit = conn.get_doccount()  - start
    
    results =  conn.search(query, start, limit + start)
    print "ID\tRANK\tDATA"
    print "--"*30
    for res in results:
        print '-----------'
        print "data: %s\t%s\t" % (res.id, res.rank), "; ".join([ "%s: %s" % (k, v) for k, v in res.data.items()]) 
        print 'value:    ', '; '.join([ str(value.num) + ':' + value.value for value in res._doc.values()])
        print 'term:    ', '; '.join([ value.term for value in res._doc.termlist()])

def print_fields(database):
    """ 打印数据库中已缓存的字段 """
    print "数据库中的已索引字段:"
    print "--"*15

    conn = xappy.IndexerConnection(database)
    fields = conn.get_fields_with_actions()
    for field, value in fields:
        print field, value

    print "--"*15

def main(argv):
    
    try:
        opts, args = getopt.getopt(argv, "lfci:hs:n:", ["list", "fields", "count", "index=", "help", 'start=', 'numbers='])
    except getopt.GetoptError:
        usage()
        sys.exit(2)
    
    command = None
    database = None
    list_start = 0
    list_numbers  = 0

    for opt, arg in opts:
        if opt in ('-l', '--list'):
            command = 'list'

        if opt in ('-s', '--start'):
            try:
                list_start = int(arg)
            except:
                print "start 必须是一个整数"
                sys.exit(2)

        if opt in ('-n', '--numbers'):
            try:
                list_numbers = int(arg )
            except:
                print "list_numbers 必须是一个整数"
                sys.exit(2)
    
        if opt in ('-f', '--fields'):
            command = 'fields'

        elif opt in ('-c', '--count'):
            command = 'count'

        elif opt in ('-i', '--index'):
            database = arg

        elif opt in ('-h', '--help'):
            usage()
            break;
    
    if not opts:
        usage()
        sys.exit(2)
    
    if not database:
        print "ERROR: 必须指定索引数据库的路径"

    try:
        conn = xappy.SearchConnection(database)
    except:
        print "ERROR: 无法连接指定的索引数据库"
        sys.exit(2)

    print list_start, list_numbers
    if command == 'list':
        print_documents(conn, list_start, list_numbers)

    if command == 'fields':
        print_fields(database)

    if command == 'count':
        print_document_count(conn)
            
if __name__ == '__main__':
    main(sys.argv[1:])    

