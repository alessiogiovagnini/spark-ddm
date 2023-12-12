import pyspark.context
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col


spark = SparkSession.builder \
        .master("local") \
        .appName("MyFirstSparkApplication") \
        .getOrCreate()


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


if __name__ == '__main__':


    example2()


