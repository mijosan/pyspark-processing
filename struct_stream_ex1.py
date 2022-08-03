from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = (SparkSession
         .builder
         .appName("struct_streamming")
         .getOrCreate())

lines = (spark
         .readStream.format("socket")
         .option("host", "192.168.100.73")
         .option("port", 9999)
         .load())

words = lines.select(split(col("value"), "\\s").alias("word"))

checkpointDir = "C:/Users/mobigen/running-spark/result"

streamingQuery = (words
          .writeStream.format("console")
          .outputMode("append")
          .trigger(processingTime="1 second")
          .option("checkpointLocation", checkpointDir)
          .start().lastProgress().awaitTermination())