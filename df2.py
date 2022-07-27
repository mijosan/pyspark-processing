from pyspark.sql import SparkSession

schema = "Id INT, First STRING,Last STRING, Uri STRING, Published STRING, Hits INT, Campaigns ARRAY<STRING>"

data = [[1,  "Jules",  "Damji",  "https://tiny나rl.l", "1/4/2016",  4535,  ["twitter", "Linkedln"]], [2,  "Jules",  "Damji",  "https://tiny나rl.l", "1/4/2016",  4535,  ["twitter", "Linkedln"]], [3,  "Jules",  "Damji",  "https://tiny나rl.l", "1/4/2016",  4535,  ["twitter", "Linkedln"]]]

spark = (SparkSession
         .builder
         .appName("PythonMnMCount")
         .getOrCreate())

blogs_df = spark.createDataFrame(data, schema)

blogs_df.show()

print(blogs_df.printSchema())