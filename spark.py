import os
import configparser
from pyspark.sql import SparkSession 
from pyspark.sql.types import IntegerType
from pyspark.sql.types import *
from pyspark.sql import functions as F
from pyspark.sql.functions import col
from pyspark.sql.functions import *

os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages "com.amazonaws:aws-java-sdk-s3:1.12.196,org.apache.hadoop:hadoop-aws:3.3.1" pyspark-shell'
configFilePath = r'C:\Users\kinan\.aws\credentials'
config = configparser.ConfigParser()
config.read(os.path.expanduser(configFilePath))
access_key = config.get("default", "aws_access_key_id")
secret_key = config.get("default", "aws_secret_access_key")
#session_token = "https://pinterest-data-6caaf6b1-2aef-4376-93c5-e713a3717d92.s3.amazonaws.com/2022-08-19/"

if __name__ == "__main__":
    temp= r"C:\Users\kinan\AiCore\PDPP\PDPP\temp"
    spark = SparkSession \
        .builder \
        .config("spark.local.dir", temp)\
        .appName("ReadGoogleTrendsData") \
        .master("local[*]") \
        .getOrCreate()
        

    sc=spark.sparkContext


    hadoopConf = sc._jsc.hadoopConfiguration()
    hadoopConf.set('fs.s3a.access.key', access_key)
    hadoopConf.set('fs.s3a.secret.key', secret_key)
    hadoopConf.set('spark.hadoop.fs.s3a.aws.credentials.provider', 'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider')


    df = spark.read.json("s3a://pinterest-data-6caaf6b1-2aef-4376-93c5-e713a3717d92/2022-08-19/*.json").dropDuplicates()

    # df = df.withColumn("follower_count", when(col('follower_count').like("%K"), (regexp_replace('follower_count', 'K', '').cast('int')*1000))\
    # .when(col('follower_count').like("%M"), (regexp_replace('follower_count', 'M', '').cast('int')*1000000))\
    # .when(col('follower_count').like("%B"), (regexp_replace('follower_count', 'B', '').cast('int')*1000000000))\
    # .otherwise((regexp_replace('follower_count', ' ', '').cast('int'))))


    df = df.withColumn('follower_count', 
      when(df.follower_count.endswith('k'),regexp_replace(df.follower_count,'k','000').cast('int')) \
     .when(df.follower_count.endswith('M'),regexp_replace(df.follower_count,'M','000000').cast('int')) \
     .when(df.follower_count.endswith('B'),regexp_replace(df.follower_count,'B','000000000').cast('int')) \
     .otherwise(df.follower_count)) 





    #df = df.withColumn("follower_count" ,  df["follower_count"].cast(IntegerType()))  
    df.printSchema()
    df.dtypes
    print("hello")
    # df.select("category").show(50,truncate=False)
    # df.select("description").show(50,truncate=False)
    # df.select("downloaded").show(50,truncate=False)
    # df.select("follower_count").show(50,truncate=False)
    # df.select("image_src").show(50,truncate=False)
    # df.select("index").show(50,truncate=False)
    # df.select("is_image_or_video").show(50,truncate=False)
    # df.select("tag_list").show(50,truncate=False)
    # df.select("title").show(50,truncate=False)
    #df.select("unique_id").show(500,truncate=False)
    df.orderBy("follower_count").select("follower_count").show(500,truncate=False)


    #df.show()
    # remove duplicates

    spark.stop()

    
  