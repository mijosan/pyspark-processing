import sys

from pyspark.sql import SparkSession

spark = (SparkSession
         .builder
         .appName("PythonMnMCount")
         .getOrCreate())

# 명령행 인자에서 M&M 데이터가 들어 있는 파일 이름을 얻는다.
mnm_file = "C:/Users/mobigen/running-spark/data/mnm_dataset.csv"

mnm_df = (spark.read.format("csv")
          .option("header", "true")
          .option("inferSchema", "true")
          .load(mnm_file))

count_mnm_df = (mnm_df
                .select("State", "Color", "Count")
                .collect())

print(count_mnm_df)
count_mnm_df.show(n=60, truncate=False)
print("Total Rows = %d" % (count_mnm_df.count()))
