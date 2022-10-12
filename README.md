# pyspark-demo

# """Set the Environment Variables"""

import os
os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-8-openjdk-amd64"
os.environ["SPARK_HOME"] = "/content/spark-3.1.3-bin-hadoop3.2"

# """Locate spark just to make sure command doesnt error out ?"""

import findspark
findspark.init()

# """create spark context"""

from pyspark.sql import SparkSession
spark = SparkSession.builder\
        .master("local")\
        .appName("demo")\
        .config('spark.ui.port', '4050')\
        .getOrCreate()

spark

import pyspark.sql.functions as F

raw_logs = spark.read.option("header","false").option("delimiter", " ").csv("/content/drive/MyDrive/log_data_ip_request.txt")
raw_logs.show(5, False)

log_details = (raw_logs.select(
    F.monotonically_increasing_id().alias('row_id'),
    F.col("_c0").alias("ip"),
    F.split(F.col("_c3")," ").getItem(0).alias("datetime"),
    F.split(F.col("_c5"), " ").getItem(0).alias("method"),
    F.split(F.col("_c5"), " ").getItem(1).alias("request"),
    F.col("_c6").alias("status_code"),
    F.col("_c7").alias("size"),
    F.col("_c8").alias("referrer"),
    F.col("_c9").alias("user_agent")
  
    
    
    ))

log_details.show()

from pyspark.sql.functions import regexp_replace
raw_data=log_details.withColumn('datetime', regexp_replace('datetime', '\[|\]|', ''))

#raw_data.show(truncate=False)

raw_data.write.mode("overwrite").saveAsTable("raw_data1")
spark.table("raw_data1").show(10, False)

from pyspark.sql.functions import *
date_cast = raw_data.withColumn('datetime',to_timestamp('datetime','dd/MMM/yyyy:HH:mm:ss'))

date_cast.show(truncate=False)

date_cast.printSchema

# """#clensed"""

from pyspark.sql.functions import when
data_clean=raw_data.withColumn('referrer_present', when(col('referrer') == '-', "No").otherwise("Yes"))

data_clean.show()

clean_data=data_clean.drop("referrer")

clean_data.write.mode("overwrite").saveAsTable("clean_data1")
spark.table("clean_data1").show(10, False)

"""#curated"""

clean_data.show()

clean_data.write.csv("clean_data.csv")

clean_data.printSchema

"""#**Aggregration**"""

data_head=clean_data.select("method").distinct().show()

cnt_cond = lambda cond: sum(when(cond, 1).otherwise(0))

log_per_device = date_cast.filter(col("user_agent")!="").withColumn("day_hour", hour(col("datetime"))) \
    .groupBy("user_agent") \
    .agg(count(col('row_id')).alias("row_id"), \
         first("day_hour").alias("day_hour"), \
         count(col('ip')).alias("ip"), \
         cnt_cond(col('method') == "GET").alias("no_get"), \
         cnt_cond(col('method') == "POST").alias("no_post"), \
         cnt_cond(col('method') == "HEAD").alias("no_head"), \
        ) \
     .orderBy(col("row_id").desc())

log_per_device.show()

log_per_device.write.csv("log_per_device.csv")

log_agg_across_device = date_cast.filter(col("user_agent")!="").withColumn("day_hour", hour(col("datetime"))) \
    .select((max(col('row_id'))+1).alias("row_id"), \
         avg("day_hour").alias("day_hour"), \
         count(col('ip')).alias("ip"), \
         cnt_cond(col('method') == "GET").alias("no_get"), \
         cnt_cond(col('method') == "POST").alias("no_post"), \
         cnt_cond(col('method') == "HEAD").alias("no_head"), \
        )

log_agg_across_device.show()

log_agg_across_device.write.csv("log_agg_across_device.csv")

pip install snowflake

pip install snowflake-connector-python

import os

os.environ["pyspark_submit_args"] = "--master local[2] pyspark-shell"

spark=SparkSession.builder.master("local[2]") \
.config("spar.jars","/content/snowflake-jdbc-3.13.23.jar,/content/spark-snowflake_2.12-2.11.0-spark_3.1.jar")\
.getOrCreate()

sfOptions = {
  "sfURL" : "https://fi73281.ap-south-1.aws.snowflakecomputing.com/",
  "sfUser" : "*****",
  "sfPassword" : "*****",
  "sfDatabase" : "MY_DATABASE",
  "sfSchema" : "csv",
  "sfWarehouse" : "COMPUTE_WH "
}

SNOWFLAKE_SOURCE_NAME = "net.snowflake.spark.snowflake"

df = spark.read.format(SNOWFLAKE_SOURCE_NAME).load("/content/sample_data/mnist_train_small.csv")

