import csv

import pyspark.context
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, avg
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, ArrayType

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
    return spark.read.options(header=True, inferSchema='True').schema(book_schema).csv("books_data.csv", sep=',')


def read_review_df() -> pyspark.sql.DataFrame:
    return spark.read.options(header=True, inferSchema='True').schema(review_schema).csv("Books_rating.csv", sep=',')


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


def average_review():
    df: pyspark.sql.DataFrame = read_review_df()
    (df.groupby("Title")
     .avg("review/score")
     .sort(avg("review/score"), ascending=False)
     .filter(col("avg(review/score)") <= 5).show())  # there are some exception where the score is more than 5,
    # so they are excluded


def count_publisher():
    df: pyspark.sql.DataFrame = read_books_df()
    (df.filter(col("publisher").isNotNull())
     .groupby("publisher")
     .count()
     .sort("count", ascending=False).show())


def get_cheap_price():
    df: pyspark.sql.DataFrame = read_review_df()
    (df.filter(col("Price").isNotNull())
     .groupby("Title")
     .avg("Price").sort("avg(Price)").show())


# TODO get the people that leave more review
def get_reviewer_with_most_reviews():
    df: pyspark.sql.DataFrame = read_review_df()
    df2: pyspark.sql.DataFrame = read_review_df()

    dist: pyspark.sql.DataFrame = df2.dropDuplicates(["User_id", "profileName"])

    (df.filter(col("User_id").isNotNull())
     .groupby("User_id")
     .count()
     .sort("count", ascending=False)
     .limit(20)
     .join(other=dist, on="User_id", how="inner")
     .select(["profileName", "count"]).show())


def get_most_reviewed_book():
    df: pyspark.sql.DataFrame = read_review_df()
    df2: pyspark.sql.DataFrame = read_review_df()
    rev = df2.filter(col("review/score").isNotNull()).groupby("Title").avg("review/score")
    (df.filter(col("Title").isNotNull())
     .groupby("Title")
     .count()
     .join(other=rev, on="Title", how="inner")
     .sort("count", ascending=False).show())


def get_publisher_with_most_reviewed_book():
    pass


if __name__ == '__main__':
    # info_on_data() # get info

    # average_review() # not very complicated
    # count_publisher() # not very complicated
    # get_cheap_price() # not very complicated
    # get_reviewer_with_most_reviews() # OK
    # get_most_reviewed_book() # OK
    pass
