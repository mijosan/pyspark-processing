from pyspark.sql.types import *
from pyspark.sql import *
from pyspark.sql.functions import *

spark = (SparkSession
         .builder
         .appName("struct_streamming")
         .getOrCreate())

fileSchema = (StructType()
              .add(StructField("key", IntegerType()))
              .add(StructField("value", IntegerType())))

inputDirectoryOfJsonFiles = "C:/Users/mobigen/running-spark/data/json"

inputDF = (spark
           .readStream
           .format("json")
           .schema(fileSchema)
           .load(inputDirectoryOfJsonFiles)
           )

checkpointDir = "C:/Users/mobigen/running-spark/result"

outputDirectoryOfJsonFiles = "C:/Users/mobigen/running-spark/data/json/result"

outputDF = (inputDF.writeStream
            .format("json")
            .option("path", outputDirectoryOfJsonFiles)
            .option("checkpointLocation", checkpointDir)
            .start())
