#!/usr/bin/env python

import os,sys
import string
import re
import pickle

stopword_file = './stopwords'

class CleanTweet(object):
    """
        case-sensitive, removed url, hashtag#, special term like 'RT', and reply@
    """
    _url = r'http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\(\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+'
    _tweeterid = r'@\w{1,15}'
    _retweet = r'^RT'
    _nonascii = r'[^\x00-\x7F]+'
    filter_re = [_url, _tweeterid, _retweet, _nonascii]

    def __init__(self, rawtweet, stopwords=[]):
        self._tweet = rawtweet
        cleantweet = rawtweet
        for ptn in self.filter_re:
            cleantweet = re.sub(ptn, '', cleantweet)
        punct = string.punctuation#.replace("'","")
        cleantweet = cleantweet.translate(None, punct)
        self._toks = cleantweet.lower().replace('\xe2','').split()
        self._toks = [item.strip() for item in self._toks if item not in stopwords]
        for w in self._toks:
            if '\xe2' in w:
                print w
        self._cleantweet = ' '.join(self._toks)
        

    def rawtweet(self):
        return self._tweet

    def cleantweet(self):
        return self._cleantweet
    
    def toks(self):
        return self._toks

    def __str__(self):
        return self._cleantweet

infilename = sys.argv[1]
outfilename = sys.argv[2]
tweets = []
stopwords = []
with open(stopword_file, 'rb') as fs:
    for word in fs:
        stopwords.append(word.strip()) 
    fs.close()

with open(infilename, 'rb') as fi, open(outfilename, 'wb') as fo:
    infile = fi.read()
    start = '['
    stop = ']'
    buf = ''
    flag = False
    for c in infile:
        if c == start:
            flag = True
            continue
        elif c == stop:
            tweetobj = CleanTweet(buf, stopwords).cleantweet()
            if tweetobj != '':
                tweets.append(tweetobj)
            buf = ''
            flag = False
        if flag:
            buf += c
        if len(tweets) >= 1000000:
            break
    pickle.dump(tweets, fo)
    fi.close() 
    fo.close()

with open(outfilename, 'rb') as fo:
    newlist = pickle.load(fo)
    for t in newlist:
        print t
