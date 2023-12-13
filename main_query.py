import csv

import pyspark.context
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, avg
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType


spark = SparkSession.builder \
        .master("local") \
        .appName("MyFirstSparkApplication") \
        .getOrCreate()


book_schema = (StructType()
               .add("Title", StringType())
               .add("description", StringType())
               .add("authors", StringType())
               .add("image", StringType())
               .add("previewLink", StringType())
               .add("publisher", StringType())
               .add("publishedDate", StringType())
               .add("infoLink", StringType())
               .add("categories", StringType())
               .add("ratingsCount", StringType())
               )
review_schema = (StructType()
                 .add("Id", StringType())
                 .add("Title", StringType())
                 .add("Price", FloatType())
                 .add("User_id", StringType())
                 .add("profileName", StringType())
                 .add("review/helpfulness", StringType())
                 .add("review/score", FloatType())
                 .add("review/time", StringType())
                 .add("review/summary", StringType())
                 .add("review/text", StringType())
                 )


def read_books_df() -> pyspark.sql.DataFrame:
    return spark.read.options(header=True, inferSchema='True').schema(book_schema).csv("books_data.csv")


def read_review_df() -> pyspark.sql.DataFrame:
    return spark.read.options(header=True, inferSchema='True').schema(review_schema).csv("Books_rating.csv")


# get info on data
def info_on_data():
    df_book: pyspark.sql.DataFrame = read_books_df()
    df_review: pyspark.sql.DataFrame = read_review_df()

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


def join_example():
    # join dataframes
    books_path: str = "books_data.csv"
    reviews_path: str = "Books_rating.csv"

    df: pyspark.sql.DataFrame = spark.read.option("header", True).csv(books_path)
    df2: pyspark.sql.DataFrame = spark.read.option("header", True).csv(reviews_path)

    df.join(df2, "Title").show()
    # TODO: after joining the tables, need to filter
    pass


def average_review():

    df2: pyspark.sql.DataFrame = read_review_df()
    df2.groupby("Title").avg("review/score").sort(avg("review/score"), ascending=False).show()
    pass


def example3():
    books_path: str = "books_data.csv"
    df: pyspark.sql.DataFrame = spark.read.options(header=True, inferSchema='True').csv(books_path)

    columns = [df["Title"], df["description"], df["authors"], df["infoLink"]]
    df.select(columns).filter(col("authors").contains("Julie")).show()


if __name__ == '__main__':
    # info_on_data()
    # join_example()
    # average_review()
    pass


