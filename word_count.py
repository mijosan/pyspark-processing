from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split, count

spark = SparkSession.builder.appName("wordcount").getOrCreate()

# 텍스트 파일을 읽어서 DataFrame으로 변환
df = spark.read.text("C:/Users/ChoiTaesan/pyspark-processing/data/word.txt")

# 단어로 분리하고 각 단어를 행으로 만든다
words = df.select(explode(split(df.value, " ")).alias("word"))

# 단어 수를 계산하고 출력한다
wordCounts = words.groupBy("word").agg(count("*").alias("count")).orderBy("count", ascending=False)
wordCounts.show()