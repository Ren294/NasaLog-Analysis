from pyspark.sql import SparkSession
from hdfs import InsecureClient
from pyspark.sql.functions import *
from pyspark.sql.types import *
import re

spark = SparkSession.builder \
    .appName("Finala")\
    .config("spark.sql.warehouse.dir", "/user/hive/warehouse")\
    .config("spark.sql.catalogImplementation", "hive")\
    .enableHiveSupport()\
    .getOrCreate()
sc = spark.sparkContext

client = InsecureClient('http://localhost:9870', user='nhomb')
hdfs_path = '/home/log/data/nasa/july/'
file_list = client.list(hdfs_path)

def parse_log(log):
    log_pattern = r'(\S+) - - \[(\d{2}\/\w{3}\/\d{4}:\d{2}:\d{2}:\d{2}) ([+-]\d{4})\] "(\S+) (\S+) (\S+)" (\d{3}) (\d+)'
    match = re.match(log_pattern, log)
    if match:
        return match.groups()
    else:
        return ("", "", "", "", "", "", "", "")

for file in file_list:
    rdd = sc.textFile(
        'hdfs://localhost:9000/home/log/data/nasa/july/'+file)

    logs_df = rdd.map(lambda x: parse_log(x)).toDF([
        "host", "timestamp", "tz", "method", "resource", "protocol", "responsecode", "bytes"
    ])
    logs_df = logs_df.filter(
        (col("host") != "") &
        (col("timestamp") != "") &
        (col("tz") != "") &
        (col("method") != "") &
        (col("resource") != "") &
        (col("protocol") != "") &
        (col("responsecode") != "") &
        (col("bytes") != "")
    )
    logs_df = logs_df.withColumn("timestamp", to_timestamp(
        col("timestamp"), "dd/MMM/yyyy:HH:mm:ss"))

    logs_df = logs_df.withColumn("ts_year", year(col("timestamp")).cast(IntegerType())) \
        .withColumn("ts_month", month(col("timestamp")).cast(IntegerType())) \
        .withColumn("ts_day", dayofmonth(col("timestamp")).cast(IntegerType())) \
        .withColumn("ts_hour", hour(col("timestamp")).cast(IntegerType())) \
        .withColumn("ts_minute", minute(col("timestamp")).cast(IntegerType())) \
        .withColumn("ts_sec", second(col("timestamp")).cast(IntegerType())) \
        .withColumn("ts_dayOfWeek", dayofweek(col("timestamp")).cast(IntegerType())) \
        .withColumn("clientAuthId", col("host")) \
        .withColumn("userId", col("host"))
    logs_df.write.mode("append").saveAsTable("app_log_new.log")