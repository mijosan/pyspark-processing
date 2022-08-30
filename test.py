import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = (SparkSession
         .builder
         .appName("TripDataProcessing")
         .config("spark.executor.memory", "2g")
         .config("spark.driver.memory", "2g")
         .getOrCreate())

read_file_location = "C:/Users/mobigen/running-spark/data/trip_dataset.csv" # 42mb

df = (spark.read.format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load(read_file_location))

(df.write
    .option("path", "C:/Users/mobigen/Desktop/dddf")
    .saveAsTable("trip_table"))

(df.write
    .option("path", "C:/Users/mobigen/Desktop/dddf")
    .mode("overwrite")
    .saveAsTable("trip_table"))

spark.sql("""drop table trip_table""")

df2 = spark.sql("""SELECT * FROM trip_table""")

df2.show() 

