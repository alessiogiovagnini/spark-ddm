import pyspark.context
from pyspark import SparkContext
from pyspark.sql import SparkSession


if __name__ == '__main__':

    # sc: pyspark.context.SparkContext = SparkContext("local", "count app")
    # words = sc.parallelize(
    #     ["scala",
    #      "java",
    #      "hadoop",
    #      "spark",
    #      "akka",
    #      "spark vs hadoop",
    #      "pyspark",
    #      "pyspark and spark"]
    # )
    # counts = words.count()
    # print("Number of elements in RDD -> %i" % counts)

    spark = SparkSession.builder \
        .master("local") \
        .appName("MyFirstSparkApplication") \
        .getOrCreate()

    path: str = "./books_data.csv"
    df = spark.read.option("header", True).csv(path)

    df.show()
