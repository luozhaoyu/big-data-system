#!/usr/env/python
# -*- coding: utf-8 -*-
import os
import sys
sys.path.append(os.path.join(os.environ['SPARK_HOME'], "python"))
sys.path.append(os.path.join(os.environ['SPARK_HOME'], "python/lib/py4j-0.8.2.1-src.zip"))
import argparse

import pyspark
from pyspark import SparkContext
from pyspark.sql import SQLContext, HiveContext

from run_spark_query import get_statistics, clean_collect_logs


@clean_collect_logs
def query12_no(query_name, conf=None):
    sc = SparkContext(conf=conf)
    sqlContext = HiveContext(sc)

    # SQL statements can be run by using the sql methods provided by sqlContext
    sql = "use tpcds_text_db_1_50"
    res = sqlContext.sql(sql)
    sql = """
select  i_item_desc 
      ,i_category 
      ,i_class 
      ,i_current_price
      ,i_item_id
      ,sum(ws_ext_sales_price) as itemrevenue 
      ,sum(ws_ext_sales_price)*100/sum(sum(ws_ext_sales_price)) over
          (partition by i_class) as revenueratio
from	
	web_sales
    	,item 
    	,date_dim
where 
	web_sales.ws_item_sk = item.i_item_sk 
  	and item.i_category in ('Jewelry', 'Sports', 'Books')
  	and web_sales.ws_sold_date_sk = date_dim.d_date_sk
	and date_dim.d_date between '2001-01-12' and '2001-02-11'
group by 
	i_item_id
        ,i_item_desc 
        ,i_category
        ,i_class
        ,i_current_price
order by 
	i_category
        ,i_class
        ,i_item_id
        ,i_item_desc
        ,revenueratio
limit 100
    """
    @get_statistics
    def execute_sql(query_name, sqlContext, sql):
        res = sqlContext.sql(sql)
        return res

    res = execute_sql(query_name, sqlContext, sql)
#    for each in teenagers.collect():
#        print(each[0])
    res['count'] = res['output'].count()

    sc.stop()
    return res


@clean_collect_logs
def query12_input(query_name, conf=None, output_persist=False):
    sc = SparkContext(conf=conf)
    sqlContext = HiveContext(sc)

    # SQL statements can be run by using the sql methods provided by sqlContext
    sql = "use tpcds_text_db_1_50"
    res = sqlContext.sql(sql)
    @get_statistics
    def execute_sql(query_name, sqlContext):
        web_sales_sql = "select * from web_sales"
        web_sales = sqlContext.sql(web_sales_sql)
        web_sales.persist()
        web_sales.registerAsTable("web_sales")
        item_sql = "select * from item"
        item = sqlContext.sql(item_sql)
        item.persist()
        item.registerAsTable("item")
        date_dim_sql = "select * from date_dim"
        date_dim = sqlContext.sql(date_dim_sql)
        date_dim.persist()
        date_dim.registerAsTable("date_dim")
        sql = """
    select  i_item_desc 
          ,i_category 
          ,i_class 
          ,i_current_price
          ,i_item_id
          ,sum(ws_ext_sales_price) as itemrevenue 
          ,sum(ws_ext_sales_price)*100/sum(sum(ws_ext_sales_price)) over
              (partition by i_class) as revenueratio
    from	
        web_sales
            ,item 
            ,date_dim
    where 
        web_sales.ws_item_sk = item.i_item_sk 
        and item.i_category in ('Jewelry', 'Sports', 'Books')
        and web_sales.ws_sold_date_sk = date_dim.d_date_sk
        and date_dim.d_date between '2001-01-12' and '2001-02-11'
    group by 
        i_item_id
            ,i_item_desc 
            ,i_category
            ,i_class
            ,i_current_price
    order by 
        i_category
            ,i_class
            ,i_item_id
            ,i_item_desc
            ,revenueratio
    limit 100
        """
        res = sqlContext.sql(sql)
        if output_persist:
            res.persist()
        return res

    res = execute_sql(query_name, sqlContext)
    res['count'] = res['output'].count()

    sc.stop()
    return res


def run_all():
    sparkConf = pyspark.SparkConf()
    sparkConf.setAppName("CS-838-Assignment2-Question2")
    sparkConf.set("spark.driver.memory", "1g")
    sparkConf.set("spark.cores.max", "4")
    sparkConf.set("spark.executor.memory", "21000m")
    sparkConf.set("spark.task.cpus", "1")
    sparkConf.set("spark.eventLog.enabled", "true")
    sparkConf.set("spark.eventLog.dir", "/home/ubuntu/storage/logs")
    with open("question2.txt", "w") as f:
        res = str(query12_no("persist_no", conf=sparkConf))
        f.write(res + '\n')
        res = str(query12_input("persist_input", conf=sparkConf))
        f.write(res + '\n')
        res = str(query12_input("persist_output", conf=sparkConf, output_persist=True))
        f.write(res + '\n')


def main():
    parser = argparse.ArgumentParser(description="""
    for question 2
        """)
    parser.add_argument("-q", "--query", help="query type: no|input|output", default="no")
    args = parser.parse_args()

    run_all()
#    if args.query == "no":
#        with open("persist_no", "w") as f:
#            res = str(query12_no(sparkConf))
#            print res
#            f.write(res)
#    elif args.query == "input":
#        with open("persist_input", "w") as f:
#            res = str(query12_input(sparkConf))
#            print res
#            f.write(res)
#    elif args.query == "output":
#        with open("persist_input", "w") as f:
#            res = str(query12_output(sparkConf))
#            print res
#            f.write(res)


if __name__ == '__main__':
    main()
