import csv

import pyspark.context
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode


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


def example3():
    books_path: str = "books_data.csv"
    df: pyspark.sql.DataFrame = spark.read.option("header", True).csv(books_path)

    columns = [df["Title"], df["description"], df["authors"], df["infoLink"]]
    df.select(columns).filter(col("authors").contains("Julie")).show()


if __name__ == '__main__':

    example3()
    # data = []
    # with open("books_data.csv") as file_obj:
    #
    #     reader_obj = csv.reader(file_obj)
    #     for row in reader_obj:
    #
    #         if type(row[2]) is str and row:
    #             arr = row[2].strip('][').split(', ')
    #             # print("--------")
    #             # print(row[2])
    #             # print(arr)
    #             lines = []
    #             for i, v in enumerate(row):
    #                 if i == 2:
    #                     lines.insert(i, arr)
    #                 else:
    #                     lines.insert(i,row[i])
    #             data.append(lines)
    #         else:
    #             print(type(row[2]))
    #             data.append(row)

    # with open("books_data_new.csv", "w") as csv_file:
    #     writer = csv.writer(csv_file)
    #     for d in data:
    #         print(type(d))
    #         writer.writerow(d)


