import pyspark.context
from pyspark import SparkContext
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import col


book_path: str = "books_data.csv"
review_path: str = "Books_rating.csv"

spark = SparkSession.builder \
        .master("local") \
        .appName("MyFirstSparkApplication") \
        .getOrCreate()


def example():
    # example of spark query

    path: str = "../books_data.csv"
    df = spark.read.option("header", True).csv(path)

    df.show()


def get_book_info(book: str) -> list[Row]:

    df: pyspark.sql.DataFrame = spark.read.option("header", True).csv(book_path)
    columns = [df["Title"], df["description"], df["authors"], df["infoLink"]]

    res = df.select(columns).filter(col("Title").contains(book)).collect()

    return res




