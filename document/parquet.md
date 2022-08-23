# 파케이
- 파케이 파일은 데이터 파일, 메타데이터, 여러 압축 파일 및 일부 상태 파일이 포함된 디렉터리 구조에 저장된다.
- 푸터의 메타데이터에는 파일 형식의 버전, 스키마, 경로 등의 칼럼 데이터가 포함된다.
- 파케이가 메타데이터의 일부로 저장하기 때문에, 스트리밍 데이터 소스에서 읽는 경우가 아니면 스키마를 제공할 필요가 없다.

# 파케이 파일 읽기
- df = spark.read.format("parquet").load(file)

# 데이터 프레임을 파케이 파일로 쓰기
- (df.write.format("parquet")
    .mode("overwrite")
    .option("compression", "snappy")
    .save("/tmp/data/parquet/df_parquet"))

# 스파크 SQL 테이블에 데이터 프레임 쓰기
- (df.write
    .mode("overwrite")
    .saveAsTable("us_delay_flights_tbl))