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
    read_review_df().createOrReplaceTempView("REVIEWS")
    (spark.sql("""
    SELECT
        *
    FROM REVIEWS
    WHERE Title IS NOT NULL
    """)
     .withColumnsRenamed({"review/score": "score"})
     ).createOrReplaceTempView("SCORE")
    # it is necessary to rename the column because
    # it has a slash and SQL cannot properly read it
    spark.sql("""
     SELECT
        Title,
        AVG(score)
     FROM SCORE
     WHERE score IS NOT NULL
     GROUP BY Title
     """).show()

    # query with function
    # df: pyspark.sql.DataFrame = read_review_df()
    # (df.groupby("Title")
    #  .avg("review/score")
    #  .sort(avg("review/score"), ascending=False)
    #  .filter(col("avg(review/score)") <= 5)).show()
    # there are some exception where the score is more than 5, so they are excluded


def books_by_publisher():
    read_books_df().createOrReplaceTempView("BOOKS")
    spark.sql("""
    SELECT 
        publisher,
        COUNT(*) AS publishedBooks
    FROM BOOKS 
    WHERE publisher IS NOT NULL
    GROUP BY publisher
    ORDER BY publishedBooks DESC
    """).show()

    # query with function
    # df: pyspark.sql.DataFrame = read_books_df()
    # (df.filter(col("publisher").isNotNull())
    #  .groupby("publisher")
    #  .count()
    #  .sort("count", ascending=False)).show()


def get_cheap_price():
    read_review_df().createOrReplaceTempView("BOOKS")
    spark.sql("""
    SELECT
        TITLE,
        AVG(Price) as price
    FROM BOOKS
    WHERE Price IS NOT NULL
    GROUP BY Title
    ORDER BY price ASC
    """).show()

    # df: pyspark.sql.DataFrame = read_review_df()
    # return (df.filter(col("Price").isNotNull())
    #         .groupby("Title")
    #         .avg("Price").sort("avg(Price)"))


def get_reviewer_with_most_reviews():
    df: pyspark.sql.DataFrame = read_review_df()
    df2: pyspark.sql.DataFrame = read_review_df()
    dist: pyspark.sql.DataFrame = df2.dropDuplicates(["User_id", "profileName"])

    return (df.filter(col("User_id").isNotNull())
            .groupby("User_id")
            .count()
            .join(other=dist, on="User_id", how="inner")
            .select(["profileName", "count"])
            .sort("count", ascending=False)
            )


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
    book_num = book_pub_title.groupby("publisher").count().filter(col("count") >= 10)
    average_rev = df.groupby("Title").avg("review/score").filter(col("avg(review/score)") <= 5)
    return (book_pub_title.join(other=average_rev, on="Title")
            .groupby("publisher")
            .avg("avg(review/score)")
            .select("publisher", col("avg(avg(review/score))").alias("average-score"))
            .join(other=book_num, on="publisher", how="inner")
            .select(["publisher", "average-score"])
            .sort("average-score", ascending=False)
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
    # uncomment the following lines to choose which query to run!

    # info_on_data()  # get info

    # average_review()  # IN SQL
    # books_by_publisher()  # IN SQL
    # get_cheap_price()  # IN SQL

    # users_that_posted_at_least_one_review()  # dataframe
    # get_reviewer_with_most_reviews().show()  # dataframe
    # get_most_reviewed_book().show()  # dataframe
    # get_publisher_with_best_reviewed_book().show()  # dataframe
    # book_sorted_by_date().show()  # dataframe
    pass
