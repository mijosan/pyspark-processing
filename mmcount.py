import sys

from pyspark.sql import SparkSession

spark = (SparkSession
         .builder
         .appName("PythonMnMCount")
         .getOrCreate())

# 명령행 인자에서 M&M 데이터가 들어 있는 파일 이름을 얻는다.
mnm_file = sys.argv[1]

mnm_df = (spark.read.format("csv")
          .option("header", "true")
          .option("inferSchema", "true")
          .load(mnm_file))

