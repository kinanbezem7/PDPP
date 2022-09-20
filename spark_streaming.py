from pyspark.sql import SparkSession
import os
from pyspark.sql import SparkSession 
from pyspark.sql.types import *
from pyspark.sql import functions as F
from pyspark.sql.functions import col
from pyspark.sql.functions import *
from pyspark.sql.functions import regexp_replace
import json
import pandas as pds

HOST = 'localhost'
USER = 'admin'
PASSWORD = 'admin'
DATABASE = 'pgserver1'
PORT = 5432



os.environ["PYSPARK_SUBMIT_ARGS"] = "--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0 spark_streaming.py pyspark-shell --jar /home/kinan/anaconda3/envs/PDPP/postgresql-42.5.0.jar"
kafka_topic_name = "MyFirstKafkaTopic"
kafka_bootstrap_servers = "localhost:9092"



schema = StructType([
        StructField("category", StringType(), True),
        StructField("index", StringType(),True),
        StructField("unique_id",StringType(),True),
        StructField("title",StringType(),True),
        StructField("description",StringType(),True),
        StructField("poster_name", StringType(), True),
        StructField("follower_count", StringType(), True),
        StructField("tag_list", StringType(), True),
        StructField("is_image_or_video", StringType(), True),
        StructField("image_src", StringType(), True),
        StructField("downloaded", StringType(), True),
        StructField("save_location", StringType(), True),
        ])


spark = SparkSession \
    .builder \
    .appName("Kafka") \
    .config("spark.executor.extraClassPath", "/home/kinan/anaconda3/envs/PDPP/postgresql-42.5.0.jar") \
    .getOrCreate()


#spark = spark.sparkContext.setLogLevel("ERROR")

df = spark \
    .readStream \
    .format("Kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("subscribe", kafka_topic_name) \
    .option("startingOffsets", "earliest") \
    .load()


def transform(df, epoch_id):
    df.printSchema()
    df = df.selectExpr("CAST(value AS STRING)", "CAST(timestamp AS STRING)")
    df = df.withColumn("value", from_json("value", schema)).select("value.*", "timestamp")
    
    


    df = df.withColumn('follower_count', 
      when(df.follower_count.endswith('k'),regexp_replace(df.follower_count,'k','000').cast('int')) \
     .when(df.follower_count.endswith('M'),regexp_replace(df.follower_count,'M','000000').cast('int')) \
     .when(df.follower_count.endswith('B'),regexp_replace(df.follower_count,'B','000000000').cast('int')) \
     .otherwise(df.follower_count)) 
    df = df.withColumn("downloaded",col("downloaded").cast("boolean"))
    df = df.withColumn('tag_list', regexp_replace('tag_list', 'N,o, ,T,a,g,s, ,A,v,a,i,l,a,b,l,e', 'N/A')) 
    
    df.write.format("console").save()


    df.select("*").write.format("jdbc")\
    .option("url", "jdbc:postgresql://localhost:5432/pgserver1") \
    .option("driver", "/home/kinan/anaconda3/envs/PDPP/postgresql-42.5.0.jar").option("dbtable", "experimental_data") \
    .option("user", "admin").option("password", "admin").save()

#    df.select("*").write.format("jdbc")\
#     .option("url", "jdbc:postgresql://localhost:5432/pgserver1") \
#     .option("driver", "org.postgresql.Driver").option("dbtable", "experimental_data") \
#     .option("user", "admin").option("password", "admin").save()
    return df



df.writeStream.foreachBatch(transform).start().awaitTermination()


