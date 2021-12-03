import os
import sys

from pyspark import SparkConf
from pyspark import SparkContext

from pyspark.sql import SparkSession
from pyspark.sql.functions import array, col, first, udf
from pyspark.sql.types import ArrayType, FloatType

from pyspark.ml.feature import HashingTF, IDF, Tokenizer
from pyspark.ml.linalg import SparseVector


def print_df(df):
    df.show()


def str2bool(v):
    if isinstance(v, bool):
        return v
    if v.lower() in ('yes', 'true', 't', 'y', '1'):
        return True
    elif v.lower() in ('no', 'false', 'f', 'n', '0'):
        return False


def get_document_df(sc: SparkContext):
    spark = SparkSession.builder.getOrCreate()

    rdd = sc.wholeTextFiles(os.path.join('/', sys.argv[1], '*'))
    df = spark.createDataFrame(rdd, schema=['article_name', 'article_text'])

    def remove_hdfs_path(path):
        filename = path.split('/')[-1]
        return filename.replace('.txt', '')

    remove_hdfs_path = udf(remove_hdfs_path)

    df = df.withColumn('article_name', remove_hdfs_path('article_name'))
    return df


def get_vector_df(document_df):
    tokenizer = Tokenizer(inputCol='article_text', outputCol='words')
    words_df = tokenizer.transform(document_df)

    hashing_TF = HashingTF(inputCol='words', outputCol='rawFeatures')
    vector_df = hashing_TF.transform(words_df)

    idf = IDF(inputCol='rawFeatures', outputCol='features')
    idfModel = idf.fit(vector_df)
    rescaledData = idfModel.transform(vector_df)

    return rescaledData


def get_distances_df(vector_df):
    vectors_df_1 = vector_df.select(col('article_name').alias('article_name_1'),
                                    col('features').alias('features_1'))
    vectors_df_2 = vector_df.select(col('article_name').alias('article_name_2'),
                                    col('features').alias('features_2'))
    distances_df = vectors_df_1.crossJoin(vectors_df_2)

    dot_udf = udf(lambda vectors: vectors[0].dot(vectors[1]).tolist(), FloatType())
    distances_df = distances_df.withColumn('dot', dot_udf(array(distances_df.features_1, distances_df.features_2)))

    norm_udf = udf(lambda vector: vector.norm(2).tolist(), FloatType())
    distances_df = distances_df.withColumn('v1_norm', norm_udf(distances_df.features_1))
    distances_df = distances_df.withColumn('v2_norm', norm_udf(distances_df.features_2))

    def cosine_distance(dot_norms_array):
        numerator = dot_norms_array[0]
        denominator = dot_norms_array[1] * dot_norms_array[2]

        return numerator / denominator if denominator != 0 else -1

    cosine_distance_udf = udf(cosine_distance, FloatType())
    distances_df = distances_df.withColumn('distance',
                                           cosine_distance_udf(array(
                                               distances_df.dot, distances_df.v1_norm, distances_df.v1_norm
                                           )))
    distances_df = distances_df.select(['article_name_1', 'article_name_2', 'dot', 'v1_norm', 'v2_norm', 'distance'])
    if str2bool(sys.argv[3]):
        distances_df = distances_df.withColumn('distance', udf(lambda x: (0.5*x) + 0.5)('distance'))
    return distances_df


def format_distances_df(distances_df):
    renamed_df = distances_df.select(col('article_name_1').alias('article_name'), 'article_name_2', 'distance')
    pivot_df = renamed_df.groupBy('article_name').pivot('article_name_2').agg(first('distance'))
    sorted_df = pivot_df.sort('article_name')

    return sorted_df


def main():
    conf = SparkConf()
    conf.setMaster('spark://concord:30257').setAppName('cosine_distance')
    sc = SparkContext(conf=conf)

    document_df = get_document_df(sc)

    vector_df = get_vector_df(document_df)
    print_df(vector_df)

    distances_df = get_distances_df(vector_df)
    print_df(distances_df)

    formatted_df = format_distances_df(distances_df)
    print_df(formatted_df)

    formatted_df.coalesce(1).write.csv(sys.argv[2], header=True)


if __name__ == '__main__':
    main()
