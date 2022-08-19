import sys

from pyspark.sql import SparkSession

spark = (SparkSession
         .builder
         .appName("PythonMnMCount")
         .getOrCreate())

spark.conf.set("spark.sql.shuffle.partitions", 1800)
spark.conf.set("spark.sql.files.maxPartitionBytes", 62750)

# 명령행 인자에서 M&M 데이터가 들어 있는 파일 이름을 얻는다.
mnm_file = "C:/Users/mobigen/running-spark/data/mnm_dataset.csv"

mnm_df = (spark.read.format("csv")
          .option("header", "true")
          .option("inferSchema", "true")
          .load(mnm_file))

# 파티션 개수 spark.sql.files.maxpartitionBytes 의 옵션이 128MB이기 때문에 파티션은 1개다
print(f'{mnm_df.rdd.getNumPartitions()}{"개"}')

# # 리파티션
# rp_mnm_df = mnm_df.repartition(6)
# print(f'{rp_mnm_df.rdd.getNumPartitions()}{"개"}')

# # 셔플하면 파티션 변화 spark.sql.shuffle.partitions 기본값은 200
sf_mnm_df = (mnm_df.groupBy("State")
                .count())
# print(sf_mnm_df.show())
print(f'{sf_mnm_df.rdd.getNumPartitions()}{"개"}')

while True:
    print("")
