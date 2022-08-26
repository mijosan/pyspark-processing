import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = (SparkSession
         .builder
         .appName("TripDataProcessing")
         .config("spark.executor.memory", "2g")
         .config("spark.driver.memory", "2g")
         .getOrCreate())

read_file_location = "C:/Users/mobigen/running-spark/data/trip_dataset.csv" # 42mb
write_file_location = "C:/Users/mobigen/running-spark/data/result"

df = (spark.read.format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load(read_file_location))

# 데이터를 읽어서 parquet 형식 파일로 저장
# 코어 개수가 8개 이므로 spark.default.parallelism 값은 8 
# -> 각각의 스태이지에서 태스크의 개수도 8개 생성
# -> 각각의 태스크들은 데이터를 나눠서 load 한다.
# -> 결과적으로 파케이 파일은 8개의 파티션으로 저장 된다.
# WARN MemoryManager: Total allocation exceeds 95.00% (1,020,054,720 bytes) of heap memory 발생 
# -> config("spark.executor.memory", "2g") 설정
(df.write.format("parquet")
    .mode("overwrite")
    .save(write_file_location + "/" + "default_location"))

# Start Station 별로 partition 하여 저장
# 각각 디렉토리가 생성되어 8개의 파티션 파일이 생성됨
(df.write.format("parquet")
    .mode("overwrite")
    .partitionBy("Start Station")
    .save(write_file_location + "/" + "partitionBy_start_station"))

# (non-partition으로) 데이터 셋을 1개의 parquet 파일로 저장
(df.coalesce(1).write.format("parquet")
    .mode("overwrite")
    .save(write_file_location + "/" + "non_partition"))

# (non-partition으로) 데이터 셋을 5개의 파일로 나누어 저장
(df.coalesce(5).write.format("parquet")
    .mode("overwrite")
    .save(write_file_location + "/" + "5_partition"))

# csv로 적재된 원본 데이터 셋을 partition당 사이즈가 10MB를 넘지 않도록
spark.conf.set("spark.sql.files.maxPartitionBytes", 1000000)
(df.write.format("csv")
    .mode("overwrite")
    .save(write_file_location + "/" + "10M_under_partition"))

# (non-partition으로) 데이터 셋을 파일당 10만개 Row를 보관하도록 저장
# 무조건 파티션을 1로 만들고 사용해야 maxRecordsPerFile 옵션이 의미가 있다 아니면 파티션만큼 파일이 나눠진다
(df.coalesce(1).write.format("csv")
    .option("header","true")
    .option("maxRecordsPerFile", 100000)
    .mode("overwrite")
    .save(write_file_location + "/" + "10_row_partition"))

# "Start Date" 컬럼을 기준으로 partition 형태로 저장
(df.withColumn("Start_Date", from_unixtime(unix_timestamp("Start Date", "MM/dd/yyyy HH:mm")))
    .withColumn("Year", substring("Start_Date", 1, 4))
    .withColumn("Month", substring("Start_Date", 6, 2))
    .withColumn("Day", substring("Start_Date", 9, 2))
    .write.format("parquet")
    .mode("overwrite")
    .partitionBy("Start_Date")
    .save(write_file_location + "/" + "year_month_day_partition"))



os.system("pause")