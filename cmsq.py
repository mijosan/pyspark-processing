from email import header
from pyspark.sql import SparkSession
from pyspark.sql.functions import expr

spark = (SparkSession
         .builder
         .appName("airportProcessing")
         .getOrCreate())

airportsnaFilePath = "C:/Users/mobigen/running-spark/data/airport-codes-na.txt"
tripdelaysFilePath = "C:/Users/mobigen/running-spark/data/departuredelays.csv"

# 공항 데이터세트 읽어 오기
airportsna = (spark.read
              .format("csv")
              .options(header="true", inferSchema="true", sep="\t")
              .load(airportsnaFilePath))

airportsna.createOrReplaceTempView("airports_na")

# 출발 지연 데이터세트 읽어 오기
departureDelays = (spark.read
                   .format("csv")
                   .options(header="true")
                   .load(tripdelaysFilePath))

departureDelays = (departureDelays
                   .withColumn("delay", expr("CAST(delay as INT) as delay"))
                   .withColumn("distance", expr("CAST(distance as INT) as distance")))

departureDelays.createOrReplaceTempView("departureDelays")

# 임시 작은 테이블 생성
foo = (departureDelays
       .filter(expr("""origin == 'SEA' AND destination == 'SFO' and date like '01010%' and delay > 0""")))

foo.createOrReplaceGlobalTempView("foo")

airportsna.show()
departureDelays.show()
foo.show()

# Union
bar = departureDelays.union(foo)
bar.createOrReplaceGlobalTempView("bar")

# 결합된 결과 보기 (특정 시간 범위에 대한 SEA와 SFO를 필터)
bar.filter(expr("""origin == 'SEA' AND destination == 'SFO' AND date LIKE '01010%' AND delay > 0""")).show()

# Join
(foo.join(airportsna, airportsna.IATA == foo.origin)
    .select("City", "State", "date", "delay", "distance", "destination")
    .show())






