여러 Executor에서 분산으로 JDBC select 또는 write 하기

​

​

numPartitions : 테이블 읽기 및 쓰기에서 병렬 처리를 위해 사용할 수 있는 최대 파티션 수이다. 이것은 또한 최대 동시 JDBC 연결 수를 결정

partitionColumn : 외부 소스를 읽을 때 partitionColumn은 파티션을 결정하는 데 사용되는 칼럼이고 partitionColumn은 숫자, 날짜 또는 타임스탬프 칼럼이어야 한다.

lowerBound : 파티션 크기에 대한 파티션 열의 최솟값을 설정

upperBound : 파티션 크기에 대한 파티션 열의 최대값을 설정

​

추천 설정)

numpartitions의 좋은 시작점은 스파크 워커 수의 배수를 사용

만약 모든 값이 2000에서 4000 사이인 경우 10개의 쿼리 중 2개만 모든 작업을 수행한다 따라서  lowerBound와 upperBound를 2000, 4000으로 설정

데이터 스큐를 방지하기 위해 균일하게 분산될 수 있는 파티션칼럼을 선택하고 가능한 경우 파티션을 더 균등하게 분산하기 위해 새 항목을 생성하도록 한다. (여러 칼럼값의 해시)

​

​

예제 코드)

만약 executor가 10개라고 가정하면,

하나의 executor 에서 "select * from table where partitionColumn 1000 between 2000" 이런식으로 가져온다.

df = spark.read
  .format("jdbc")
  .option("url", mysqlUrl)
  .option("driver", "com.mysql.cj.jdbc.Driver")
  .option("user", mysqlUsername)
  .option("password", mysqlPassword)
  .option("useSSL", "false")
  .option("dbtable", table)
  .option("partitionColumn", partitionKey)
  .option("numPartitions", 10)
  .option("lowerBound", 1000)
  .option("upperBound", 10000)
  .load()