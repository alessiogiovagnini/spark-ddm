import pyspark.context
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import col


book_path: str = "books_data.csv"
review_path: str = "Books_rating.csv"

spark = SparkSession.builder \
        .master("local") \
        .appName("MyFirstSparkApplication") \
        .getOrCreate()


def get_book_info(book: str, limit=100) -> list[Row]:

    df: pyspark.sql.DataFrame = spark.read.option("header", True).csv(book_path)
    columns = [df["Title"], df["description"], df["authors"], df["infoLink"]]

    res = df.select(columns).filter(col("Title").contains(book)).take(limit)

    return res


def get_book_reviews(book_title: str, limit=100) -> list[Row]:
    df: pyspark.sql.DataFrame = spark.read.option("header", True).csv(review_path)
    columns = [df["review/helpfulness"], df["review/score"], df["review/summary"], df["review/text"]]

    res = df.select(columns).filter(df.Title == book_title).take(limit)

    return res


def get_books_from_author(author: str, limit=100) -> list[Row]:
    df: pyspark.sql.DataFrame = spark.read.option("header", True).csv(book_path)
    columns = [df["Title"], df["description"], df["authors"], df["infoLink"]]
    res = df.select(columns).filter(col("authors").contains(author)).take(limit)
    return res
