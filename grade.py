from pyspark.sql import SparkSession
from pyspark.sql.functions import rank, desc
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.window import Window

spark = SparkSession.builder.appName("grade").getOrCreate()

schema = StructType([
    StructField("name", StringType(), True),
    StructField("score", IntegerType(), True)
])

# 텍스트 파일을 읽어서 DataFrame으로 변환
df = spark.read.format("csv").option("sep", " ").schema(schema).load("C:/Users/ChoiTaesan/pyspark-processing/data/grade.csv")

windowSpec = Window.orderBy(desc("score"))
df = df.select("name", "score", rank().over(windowSpec).alias("rank"))

# 결과 출력
df.show()