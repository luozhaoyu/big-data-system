#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os,sys
sys.path.append(os.path.join(os.environ['SPARK_HOME'], "python"))
sys.path.append(os.path.join(os.environ['SPARK_HOME'], "python/lib/py4j-0.8.2.1-src.zip"))
import pyspark
from pyspark import SparkContext
# for TF-IDF
from pyspark.mllib.feature import HashingTF
from pyspark.mllib.feature import IDF
# for KMeans
from pyspark.mllib.clustering import KMeans, KMeansModel
from numpy import array
from math import sqrt

sc = SparkContext()

def f(x): 
    print x
# Load documents (one per line).
"""
documents = sc.textFile("./cleantweet100w.txt").map(lambda line: line.split(" "))

hashingTF = HashingTF(50000)
tf = hashingTF.transform(documents)

tf.cache()
idf = IDF().fit(tf)
tfidf = idf.transform(tf)
"""

documents = sc.textFile("./test1out.txt").map(lambda line: line.split(" "))
tweets_list = documents.collect()

hashingTF = HashingTF(50)
tf = hashingTF.transform(documents)
tf.cache()
idf = IDF().fit(tf)
tfidf = idf.transform(tf)

sparse_data = tfidf.collect()
cluster_nums = [10, 100, 1000]
# Build the model (cluster the data)
k = 2
model = KMeans.train(sc.parallelize(sparse_data), k, maxIterations=1000, runs=30, initializationMode="k-means||",
    seed=50, initializationSteps=5, epsilon=1e-4)


from collections import Counter
words_map = {}
for i in range(k):
    words_map[i] = []
for i in range(len(tweets_list)):
    clusterid = model.predict(sparse_data[i])
    print 'cluster id: %d' % clusterid
    words_map[clusterid] += tweets_list[i]

for i in range(k):
    word_counts = Counter(words_map[i]) #counts the number each time a word appears
    print word_counts.most_common(5)
