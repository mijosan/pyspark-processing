from turtle import update
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

wordsDF = lines.select(split(col("value"), "\\s").alias("word"))

checkpointDir = "C:/Users/mobigen/running-spark/result"

spark.conf.set("spark.cassandra.connection.host", "hostAddr")

def writeCountsToCassandra(updatedCountsDF, batchId):
    # 카산드라 배치 데이터 소스를 써서 업데이트된 숫자를 쓴다.
    (updatedCountsDF.write
        .format("org.apache.spark.sql.cassandra")
        .mode("append")
        .options(table="tableName", keyspace="keyspaceName")
        .save())

# 여러 곳에 쓰기 (매번 쓸 때마다 결과 데이터가 재계산을 시도하게 된다(입력 데이터로부터 새로 읽어 들일 것이다))
def writeCountsToMultipleLocations(updatedCountsDf, batchId):
    updatedCountsDf.persist() # 캐시해줌
    updatedCountsDf.write.format(...).save()
    updatedCountsDf.write.format(...).save()
    updatedCountsDf.unpersist()


streamingQuery = (wordsDF.writeStream
          .foreachBatch(writeCountsToCassandra)
          .outputMode("update")
          .option("checkpointLocation", checkpointDir)
          .start())