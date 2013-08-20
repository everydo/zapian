# -*- encoding: utf-8 -*-
"""
CJKSplitter - Chinese, Japanese, Korean word splitter for ZCTextIndex

(C) by www.zopen.cn, panjy@zopen.cn & others

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
#try:
#    from Products.ZCTextIndex.ISplitter import ISplitter
#    from Products.ZCTextIndex.PipelineFactory import element_factory
#except:
#    pass
#from types import StringType

import re

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
# http://zh.wikipedia.org/wiki/Unicode%E5%AD%97%E7%AC%A6%E5%B9%B3%E9%9D%A2%E6%98%A0%E5%B0%84
# http://jrgraphix.net/research/unicode_blocks.php?block=87
# http://jrgraphix.net/research/unicode_blocks.php?block=85
# http://jrgraphix.net/research/unicode_blocks.php?block=95
# http://jrgraphix.net/research/unicode_blocks.php?block=76
# http://jrgraphix.net/research/unicode_blocks.php?block=90

# more to support:
# \u30a1-\u30f6 : Japanese Katakana
# \uff10-\uff19 : full-width number
# \uff21-\uff3a : full-width alphabete
# \uff41-\uff5a
# (done) \u0392-\u03c9 : greek.

norm_table = {'　':' '}

for char_ord in range(ord(u'Ａ'), ord(u'Ｚ')+1):
    norm_table[unichr(char_ord)] = unichr(ord('A') + char_ord - ord(u'Ａ'))
for char_ord in range(ord(u'ａ'), ord(u'ｚ')+1):
    norm_table[unichr(char_ord)] = unichr(ord('a') + char_ord - ord(u'ａ'))
for char_ord in range(ord(u'０'), ord(u'９')+1):
    norm_table[unichr(char_ord)] = unichr(ord('0') + char_ord - ord(u'０'))

rxNormal = re.compile(u"[a-zA-Z0-9_\u0392-\u03c9]+|[\u4E00-\u9FFF\u3400-\u4dbf\uf900-\ufaff\u3040-\u309f\uac00-\ud7af\u0400-\u052f]+", re.UNICODE)
rxGlob = re.compile(u"[a-zA-Z0-9_\u0392-\u03c9]+[*?]*|[\u4E00-\u9FFF\u3400-\u4dbf\uf900-\ufaff\u3040-\u309f\uac00-\ud7af\u0400-\u052f]+[*?]*", re.UNICODE)

class CJKSplitter(object):

    def process(self, lst, isGlob=0, deep_split_english=False):
        result = []
        if isGlob:
          rx = rxGlob
        else:
          rx = rxNormal
        for s in lst:
            # s must be unicode!

            # normalize
            new_s = []
            for char in s:
                if char in norm_table:
                    new_s.append(norm_table[char])
                else:
                    new_s.append(char)
            s = ''.join(new_s)

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
                        # XXX here, we may lost some index, due to ZCTextIndex's phrase process
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

                        if not isGlob:
                            result.append(w[-1])
                else:
                    if deep_split_english:
                        # title 进索引的时候，只要把英文和数字分词就行
                        result.extend(self.__process_one_word(w))
                    else:
                        # title 搜索关键字，不需要分词，但是要在最后加上*，以便支持模糊搜索
                        result.append(w if not isGlob else w + '*')
        # return [word.encode('utf8') for word in result]
        return result

    def processGlob(self, lst):
        return self.process(lst, 1)

    def __process_one_word(self, word):
        """ 英文数字分词 """
        word_list = list(word)
        word_len = len(word_list)

        ### 只有一位或者两位
        if word_len <= 2:
            return [word]

        ### 大于两位

        results = []
        result = word_list
        results.append(''.join(word_list))

        # abcd
        # bcd
        # bc
        # 每次让前面pop出一位, 直到只剩下两位
        while 1:
            result.pop(0)
            results.append(''.join(result))
            if len(result) <= 2: break

        return results

if __name__ == '__main__':
   words = ['我们abs非常好ddd ａｂｃ　ｄｅｆ',
        u'"我们非常好 1212* 有033212-1" and niubi' ,]

   for word in words:
       print '=====now test:', word
       s = CJKSplitter()
       print 'no glob result:'
       for i in s.process([word], deep_split_english=True):
           print i

       print 'glob result:'
       for i in s.processGlob([word]):
           print i

