#coding=utf8
"""
CJKSplitter - Chinese, Japanese, Korean word splitter for ZCTextIndex

(C) by www.ZopeChina.com, panjy@zopechina.com & others

License: see LICENSE.txt

$Id: Exp $

DESCRIPTION OF ALGORITHM

  Continuous letters and numbers make up words.  Spaces and symbols
  separate letters and numbers into words.  This is sufficient for
  all western text.

  CJK doesn't use spaces or separators to separate words, so the only
  way to really find out what constitutes a word would be to have a
  dictionary and advanced heuristics.  Instead, we form pairs from
  consecutive characters, in such a way that searches will find only
  characters that appear more-or-less the right sequence.  For example:

    ABCDE => AB BC CD DE

  This works okay since both the index and the search query is split
  in the same manner, and since the set of characters is huge so the
  extra matches are not significant.
"""
try:
    from Products.ZCTextIndex.ISplitter import ISplitter
    from Products.ZCTextIndex.PipelineFactory import element_factory
except:
    pass

import re
from types import StringType

def getSupportedEncoding(encodings):
    for encoding in encodings:
        try:
            unicode('A', encoding)
            return encoding
        except:
            pass
    return 'utf-8'

# CJK charsets ranges, see this following pages:
#
# http://jrgraphix.net/research/unicode_blocks.php?block=87
# http://jrgraphix.net/research/unicode_blocks.php?block=85
# http://jrgraphix.net/research/unicode_blocks.php?block=95
# http://jrgraphix.net/research/unicode_blocks.php?block=76
# http://jrgraphix.net/research/unicode_blocks.php?block=90

rxNormal = re.compile(u"[a-zA-Z0-9_]+|[\u4E00-\u9FFF\u3400-\u4dbf\uf900-\ufaff\u3040-\u309f\uac00-\ud7af]+", re.UNICODE)
rxGlob = re.compile(u"[a-zA-Z0-9_]+[*?]*|[\u4E00-\u9FFF\u3400-\u4dbf\uf900-\ufaff\u3040-\u309f\uac00-\ud7af]+[*?]*", re.UNICODE)

class CJKSplitter:

    default_encoding = "utf-8"

    def process(self, lst, isGlob=0):
        result = []
        if isGlob:
          rx = rxGlob
        else:
          rx = rxNormal
        for s in lst:
            if type(s) is StringType: # not unicode
                s = unicode(s, self.default_encoding, 'ignore')
            splitted = rx.findall(s)
            for w in splitted:
                if ord(w[0]) >= 12352:  # \u3040
                    len_w = len(w)
                    if len_w == 1:
                        if isGlob:
                            result.append(w + '*')
                        else:
                            result.append(w )
                    elif len_w == 2:
                        result.append(w)
                        # XXX here, we may lost some index, duto ZCTextIndex's phrase process
                        # ZCTextIndex will check each processGlob 'ed words again. And it doesn't
                        # work with CJKSplitter. It will break the search term
                        # so I have to comment the 2 lines.
                        # if not isGlob:
                        #     result.append(w[1])
                    else:
                        i = 2
                        while i <= len_w:
                            result.append(w[i-2:i])
                            # result.append(w[i-1:i+1])
                            i += 1

                        # add the last word to the catalog
			if not isGlob:
                            result.append(w[-1])
                else:
                    result.append(w)
        # return [word.encode('utf8') for word in result]
        return result

    def processGlob(self, lst):
        return self.process(lst, 1)

gb_encoding = getSupportedEncoding(['gb18030', 'mbcs', 'gbk', 'gb2312']) 
class GBSplitter(CJKSplitter):
    default_encoding = gb_encoding

big5_encoding = getSupportedEncoding(['big5', 'mbcs'])
class BIG5Splitter(CJKSplitter):
    default_encoding = big5_encoding

try:
    element_factory.registerFactory('Word Splitter',
          'CJK splitter', CJKSplitter)
    element_factory.registerFactory('Word Splitter',
          'CJK GB splitter', GBSplitter)
    element_factory.registerFactory('Word Splitter',
          'CJK BIG5 splitter', BIG5Splitter)
except:# ValueError:
    # in case the splitter is already registered, ValueError is raised
    pass

