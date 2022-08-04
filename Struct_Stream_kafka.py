from pyspark.sql.types import *
from pyspark.sql import *
from pyspark.sql.functions import *

spark = (SparkSession
         .builder
         .appName("struct_streamming")
         .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0')
         .getOrCreate())

inputDF = (spark
           .readStream
           .format("kafka")
           .option("kafka.bootstrap.servers", "192.168.100.242:9092")
           .option("subscribe", "numeric-result")
           .load())

outputDF = (inputDF.writeStream
            .format("console")
            .outputMode("append")
            .start()
            .awaitTermination()
)
