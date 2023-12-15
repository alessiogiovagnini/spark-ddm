import pyspark.context
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, to_date, when
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StringType, FloatType

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
    return (df.groupby("Title")
            .avg("review/score")
            .sort(avg("review/score"), ascending=False)
            .filter(col("avg(review/score)") <= 5))  # there are some exception where the score is more than 5,
    # so they are excluded


def count_publisher():
    df: pyspark.sql.DataFrame = read_books_df()
    return (df.filter(col("publisher").isNotNull())
            .groupby("publisher")
            .count()
            .sort("count", ascending=False))


def get_cheap_price():
    df: pyspark.sql.DataFrame = read_review_df()
    return (df.filter(col("Price").isNotNull())
            .groupby("Title")
            .avg("Price").sort("avg(Price)"))


def get_reviewer_with_most_reviews():
    df: pyspark.sql.DataFrame = read_review_df()
    df2: pyspark.sql.DataFrame = read_review_df()

    dist: pyspark.sql.DataFrame = df2.dropDuplicates(["User_id", "profileName"])

    return (df.filter(col("User_id").isNotNull())
            .groupby("User_id")
            .count()
            .sort("count", ascending=False)
            .limit(20)
            .join(other=dist, on="User_id", how="inner")
            .select(["profileName", "count"]))


def get_most_reviewed_book():
    df: pyspark.sql.DataFrame = read_review_df()
    df2: pyspark.sql.DataFrame = read_review_df()
    rev = df2.filter(col("review/score").isNotNull()).groupby("Title").avg("review/score")
    return (df.filter(col("Title").isNotNull())
            .groupby("Title")
            .count()
            .join(other=rev, on="Title", how="inner")
            .sort("count", ascending=False))


def get_publisher_with_best_reviewed_book():
    df: pyspark.sql.DataFrame = read_review_df()
    df2: pyspark.sql.DataFrame = read_books_df()

    book_pub_title = df2.select(["Title", "publisher"]).filter(col("Title").isNotNull() & col("publisher").isNotNull())
    average_rev = df.groupby("Title").avg("review/score").filter(col("avg(review/score)") <= 5)
    return (book_pub_title.join(other=average_rev, on="Title")
            .groupby("publisher")
            .avg("avg(review/score)")
            .select("publisher", col("avg(avg(review/score))").alias("average-score"))
            )


def users_that_posted_at_least_one_review() -> int:
    df: pyspark.sql.DataFrame = read_review_df()
    total: int = df.filter(col("User_id").isNotNull()).dropDuplicates(["User_id"]).count()
    print(f"Total number of users that wrote at least 1 review: {total}")
    return total


def book_sorted_by_date(older_first=False):
    spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")  # Necessary or it throws an error!!!
    df2: pyspark.sql.DataFrame = read_books_df()
    c = F.col("publishedDate")
    return (df2.filter(col("publishedDate").isNotNull() & col("Title").isNotNull())
            .select(["Title", "publishedDate"])
            .withColumn("Date", F.coalesce(
                F.to_date(c, "yyyy-MM-dd"),
                F.to_date(c, "yyyy-MM"),
                F.lit("None")
            ))
            .filter(~col("Date").contains("None"))
            .select(["Title", "Date"])
            .sort("Date", ascending=older_first)
            )


if __name__ == '__main__':
    # info_on_data()  # get info

    # average_review().show()  # not very complicated
    # count_publisher().show()  # not very complicated
    # get_cheap_price().show()  # not very complicated
    # users_that_posted_at_least_one_review()  # kinda simple but is different from other
    # get_reviewer_with_most_reviews().show()  # OK
    # get_most_reviewed_book().show()  # OK
    # get_publisher_with_best_reviewed_book().show()  # OK
    # book_sorted_by_date().show()  # OK
    pass
