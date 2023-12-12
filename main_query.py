import pyspark.context
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col


spark = SparkSession.builder \
        .master("local") \
        .appName("MyFirstSparkApplication") \
        .getOrCreate()


if __name__ == '__main__':
    path: str = "books_data.csv"
    df: pyspark.sql.DataFrame = spark.read.option("header", True).csv(path)

    columns = [df["Title"], df["description"], df["authors"], df["infoLink"]]
    res = df.select(columns).filter(col("Title").contains("Dr. Seuss")).collect()

    for i in res:
        print(i)




