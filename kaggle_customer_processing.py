import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("marketingCampaignProcessing").getOrCreate()

read_file_location = "C:/Users/mobigen/running-spark/data/marketing_campaign.csv"

df = (
    spark.read.format("csv")
    .option("delimiter", "\t")
    .option("header", "true")
    .option("inferSchema", "true")
    .load(read_file_location)
)

dfSelected = df.select(
    col("ID").alias("id"),
    col("Year_Birth").alias("year_birth"),
    col("Education").alias("education"),
    col("Kidhome").alias("count_kid"),
    col("Teenhome").alias("count_teen"),
    col("Dt_Customer").alias("date_customer"),
    col("Recency").alias("days_last_login"),
)

dfConverted1 = dfSelected.withColumn(
    "count_children", coalesce("count_kid", lit(0)) + coalesce("count_teen", lit(0))
)

(dfConverted1.select("id", "count_kid", "count_teen", "count_children").limit(5).show())

educationInvalid = "2n Cycle"
educationDefault = "NONE"

dfConverted2 = dfConverted1.withColumn(
    "education",
    when(col("education") == lit(educationInvalid), educationDefault).otherwise(
        col("education")
    ),
)

dfConverted2.select("education").distinct().show()


# 기존 date_customer 컬럼의 값과 비교를 위해 `date_joined` 라는 다른 이름으로 컬럼 값 변환 결과를 저장합니다
# 1. 이 과정에서 `to_date` 함수를 사용해 타입을 변경하고
# 2. `add_months` 함수를 통해 72개월 (= 6년) 을 기존 값에 추가했습니다.
dfWithJoined = dfConverted2.withColumn(
    "date_joined", add_months(to_date(col("date_customer"), "dd-MM-yyyy"), 72)
)

dfWithJoined.select("date_customer", "date_joined").limit(5).show()
dfWithJoined.printSchema()

# os.system("pause")
