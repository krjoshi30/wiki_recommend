import os
import sys

from pyspark import SparkConf
from pyspark import SparkContext

from pyspark.sql import *
from pyspark.sql.functions import array, col, first, udf
from pyspark.sql.types import ArrayType, FloatType

def get_document_df(sc: SparkContext):
    spark = SparkSession.builder.getOrCreate()
    df = spark.read.csv('/term_proj_in/map_pair.csv')
    article_df = df.toDF('article_1','article2','score')
    article_df = article_df.transform(lambda x: x.strip() if isinstance(x, str) else x)
    
    renamed_df = article_df.select(col('article_1').alias('article_name'), 'article2', 'score')
    pivot_df = renamed_df.groupBy('article_name').pivot('article2').agg(first('score'))
    sorted_df = pivot_df.sort('article_name')

    return sorted_df

if __name__ == '__main__':
    conf = SparkConf()
    conf.setMaster('spark://austin:30180').setAppName('normalize')
    sc = SparkContext(conf=conf)
    formated_df = get_document_df(sc)
    formated_df.coalesce(1).write.csv(sys.argv[2], header=True)
