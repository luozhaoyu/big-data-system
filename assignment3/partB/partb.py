#!/usr/env/python
# -*- coding: utf-8 -*-
import os
import sys
sys.path.append(os.path.join(os.environ['SPARK_HOME'], "python"))
sys.path.append(os.path.join(os.environ['SPARK_HOME'], "python/lib/py4j-0.8.2.1-src.zip"))
import argparse

import pyspark
from pyspark import SparkContext

# return the pair: ("product_id", "brand_name")
def get_product_object(line):
    # a example line: "1|Product1|2011"
    toks = line.split('|')
    return [(int(toks[0]), toks[1])]

# return the pair: ("product_id", "sale_quantity")
def get_sales_object(line):
    # a example line: "14|2|10" -> "seller_id|product_id|sale_quantity"
    toks = line.split('|')
    return [(int(toks[1]), int(toks[2]))]

# num: the first "num" products that had the best sales
def query_q3(num):
    # set the HDFS path of source files
    product_filepath = "hdfs://group-2-vm1:8020/user/ubuntu/product.txt"
    sales_filepath = "hdfs://group-2-vm1:8020/user/ubuntu/sales.txt"
    # set the configuration of Spark
    sparkConf = pyspark.SparkConf()
    sparkConf.setAppName("CS-838-Assignment2-Question3")
    sparkConf.setMaster("spark://10.0.1.27:7077")
    sparkConf.set("spark.driver.memory", "1g")
    sparkConf.set("spark.cores.max", "4")
    sparkConf.set("spark.executor.memory", "21000m")
    sparkConf.set("spark.task.cpus", "1")
    sparkConf.set("spark.eventLog.enabled", "true")
    sparkConf.set("spark.eventLog.dir", "/home/ubuntu/storage/logs")
    # new a SparkContext
    sc = SparkContext(conf = sparkConf)
    # cache the source files to memory
    product_data = sc.textFile(product_filepath).cache()
    sales_data = sc.textFile(sales_filepath).cache()
    # load the plain text files and make them RDD objects
    product_info_rdd = product_data.flatMap(lambda line: get_product_object(line))
    sales_rdd = sales_data.flatMap(lambda line: get_sales_object(line))
    # group by the key: product_id; and make value be the sum of grouped result
    overall_sales_rdd = sales_rdd.groupByKey().mapValues(sum)
    # sort the list by the number of overall sales
    result = overall_sales_rdd.join(product_info_rdd).sortBy(lambda x: x[1][0], False).collect()

    # put the top "num" elements into the list
    best_sale_products = []
    for i in range(num):
        # append the product_name to the list
        best_sale_products.append(result[i][1][1])

    # save the RDD into HDFS to make it persist
    saved_rdd = sc.parallelize(best_sale_products)
    saved_rdd.setName('%dBestSaleProduct_RDD' % num)
    print "Saving the result to hdfs://130.127.132.234:8020/user/ubuntu/%dBestSaleProduct_RDD" % num
    saved_rdd.saveAsTextFile(saved_rdd.name())


def main():
    query_q3(5)

if __name__ == '__main__':
    main()
