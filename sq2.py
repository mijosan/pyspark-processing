from pyspark.sql import SparkSession

spark = (SparkSession
         .builder
         .appName("SparkSQLExampleApp")
         .getOrCreate())

csv_file = "C:/Users/mobigen/running-spark/data/departuredelays.csv"

df = (spark.read.format("csv")
      .option("inferSchema", "true")
      .option("header", "true")
      .load(csv_file))

(df.write.format("parquet")
    .mode("overwrite")
    .option("compression", "snappy")
    .save("C:/Users/mobigen/running-spark/result"))

