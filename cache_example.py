from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = (SparkSession
         .builder
         .appName("CacheExample")
         .getOrCreate())

df = (spark.range(1 * 10000000)
    .toDF("id")
    .withColumn("square", col("id") * col("id")))

df.cache()
# cache()나 persist()를 사용할 때 데이터 프레임은 모든 레코드를 접근하는 액션(ex count())을 호출하기 전까지는 완전히 캐시되지 않는다. 만약 take(1) 같은 액션을 호출하면 카탈리스트가 레코드 하나만 필요한 것으로 판단하기 때문에 해당 파티션 하나만 캐시가 될 것이다.
print(df.count())

print(df.count())


# while True:
#     print()



# 왜 파티션이 8 개로 나누어지는지
# partition write랑 read 의 차이점