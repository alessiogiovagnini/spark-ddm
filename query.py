import pyspark.context
from pyspark import SparkContext
from pyspark.sql import SparkSession


spark = SparkSession.builder \
        .master("local") \
        .appName("MyFirstSparkApplication") \
        .getOrCreate()


def example():
    # example of spark query

    path: str = "./books_data.csv"
    df = spark.read.option("header", True).csv(path)

    df.show()


def get_book_info(book: str):
    path: str = "./books_data.csv"
    pass




