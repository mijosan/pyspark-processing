from pyspark.sql import SparkSession

spark = (SparkSession
         .builder
         .appName("SparkSQLExampleApp")
         .config("spark.driver.extraClassPath", "C:/Users/mobigen/Downloads/postgresql-42.4.0.jar")
         .getOrCreate())

jdbcDF1 = (spark
           .read
           .format("jdbc")
           .option("driver", "org.postgresql.Driver")
           .option("url", "jdbc:postgresql://192.168.100.73:5432/cpo")
           .option("dbtable", "bo_sale_prc")
           .option("user", "postgres")
           .load())

jdbcDF1.show(10)