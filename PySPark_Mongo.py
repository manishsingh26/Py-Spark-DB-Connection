from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local").appName("Word Count")\
    .config('spark.jars.packages', 'org.mongodb.spark:mongo-spark-connector_2.11:2.3.1')\
    .config('spark.mongodb.input.uri', 'mongodb://10.133.209.48:27017/protean')\
    .getOrCreate()

df = spark.read.format("com.mongodb.spark.sql.DefaultSource")\
    .option("database", "protean")\
    .option("collection", "dbchangelog")\
    .load()

print(df.show())
