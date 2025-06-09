from pyspark.sql import SparkSession
import time


spark = SparkSession \
    .builder \
    .appName('simple_pyspark') \
    .getOrCreate()

schema = 'ID INT, COUNTRY STRING, HIT LONG'
df = spark.createDataFrame(data=[(1,'KOREA', 120),(2,'USA',80),(3,'JAPAN',40)], schema=schema)
df.count()

# sleep 10 minute
time.sleep(600)