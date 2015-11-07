#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Usage: spark-submit clustering.py -k 10 -d 50000 tweets_filepath

use this tool to the assignment partB

positional arguments:
  tweets_filepath       The filepath of the tweet file (one *processed* tweet
                        per line). Use parsetweet.py to get *processed* tweet
                        (no stopwords, no punctuation, etc...)

optional arguments:
  -h, --help            show this help message and exit
  -k NUM_CLUSTERS, --num_clusters NUM_CLUSTERS
                        The num of clusters of KMeans (Default: 10)
  -d DIMENSION, --dimension DIMENSION
                        The dimension of feature vectors
"""


import os,sys,argparse
import pyspark
from pyspark import SparkContext
# for TF-IDF
from pyspark.mllib.feature import HashingTF
from pyspark.mllib.feature import IDF
# for KMeans
from pyspark.mllib.clustering import KMeans, KMeansModel
from numpy import array
from math import sqrt
# for most-common wordcount
from collections import Counter


def findtopics(cleantweets_input, k, dimen, outputfile, n_topics=5):
    # set the configuration of Spark
    sparkConf = pyspark.SparkConf()
    sparkConf.setAppName("CS-838-Assignment3-PartB")
    sparkConf.set("spark.driver.memory", "16g")
    sparkConf.set("spark.cores.max", "4")
    sparkConf.set("spark.default.parallelism", "4")
    sparkConf.set("spark.executor.memory", "16g")
    # new a SparkContext
    sc = SparkContext(conf = sparkConf)
    #sc = SparkContext()

    # Load documents (one per line).
    docs = sc.textFile(cleantweets_input).map(lambda line: line.split(" "))
    tweets_list = docs.collect()
    
    # Calculate the TF-IDF feature vectors
    hashingTF = HashingTF(dimen)
    tf = hashingTF.transform(docs)
    tf.cache()
    idf = IDF().fit(tf)
    tfidf = idf.transform(tf)

    sparse_data = tfidf.collect()
    # Build the KMeans model (cluster the data)
    model = KMeans.train(sc.parallelize(sparse_data), k, 
            maxIterations=1000, runs=10, initializationMode="k-means||", 
            seed=50, initializationSteps=3, epsilon=1e-4)

    # Output the n_topics(default=5) most commonly words for each cluster
    words_map = {}
    for i in range(k):
        words_map[i] = []
    for i in range(len(tweets_list)):
        clusterid = model.predict(sparse_data[i])
        #print 'cluster id: %d' % clusterid
        words_map[clusterid] += tweets_list[i]
    with open(outputfile, 'wb') as fo:
        fo.write('The most common %d words(topics) for %d' % (n_topics, k) + 
                 ' clusters in the file: %s\n' % (cleantweets_input))
        for i in range(k):
            word_counts = Counter(words_map[i]) #counts the number each time a word appears
            fo.write (('Cluster-%d: ' % i) + word_counts.most_common(n_topics).__str__() + '\n')
        fo.close()


def main():
    parser = argparse.ArgumentParser(description="""
        use this tool to the assignment partB
        """)
    parser.add_argument("tweets_filepath", help="""
        The filepath of the tweet file (one *processed* tweet per line).
        Use parsetweet.py to get *processed* tweet (no stopwords, no punctuation, etc...)
        """)
    parser.add_argument("-k", "--num_clusters",help="The num of clusters of KMeans (Default: 10)", default=10)
    parser.add_argument("-d", "--dimension", help="The dimension of feature vectors", default=50000)
    parser.add_argument("-o", "--outputfile", help="The filename of output result", default=None)
    args = parser.parse_args()
    filepath = args.tweets_filepath
    if args.outputfile is None:
        print 'error : missing outputfile path\n'
        parser.print_help()
        return
    k = int(args.num_clusters)
    dimen = int(args.dimension)
    outputfile = args.outputfile
    findtopics(filepath,k,dimen,outputfile)

if __name__ == '__main__':
    main()
