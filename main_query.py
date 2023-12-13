import csv

import pyspark.context
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode


spark = SparkSession.builder \
        .master("local") \
        .appName("MyFirstSparkApplication") \
        .getOrCreate()


# get info on data
def info_on_data():
    books_path: str = "books_data.csv"
    reviews_path: str = "Books_rating.csv"
    df_book: pyspark.sql.DataFrame = spark.read.option("header", True).csv(books_path)
    df_review: pyspark.sql.DataFrame = spark.read.option("header", True).csv(reviews_path)

    print("book schema:")
    df_book.printSchema()
    df_book.show()
    print("review schema:")
    df_review.printSchema()
    df_review.show()


def example():
    path: str = "books_data.csv"
    df: pyspark.sql.DataFrame = spark.read.option("header", True).csv(path)

    columns = [df["Title"], df["description"], df["authors"], df["infoLink"]]
    res = df.select(columns).filter(col("Title").contains("Dr. Seuss")).collect()

    for i in res:
        print(i)


def example2():
    # join dataframes
    books_path: str = "books_data.csv"
    reviews_path: str = "Books_rating.csv"

    df: pyspark.sql.DataFrame = spark.read.option("header", True).csv(books_path)
    df2: pyspark.sql.DataFrame = spark.read.option("header", True).csv(reviews_path)
    pass


def example3():
    books_path: str = "books_data.csv"
    df: pyspark.sql.DataFrame = spark.read.option("header", True).csv(books_path)

    columns = [df["Title"], df["description"], df["authors"], df["infoLink"]]
    df.select(columns).filter(col("authors").contains("Julie")).show()


if __name__ == '__main__':
    # info_on_data()
    pass


