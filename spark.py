import os

import configparser
from pyspark.sql import SparkSession


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
        .master("local[1]") \
        .getOrCreate()

    sc=spark.sparkContext


    hadoopConf = sc._jsc.hadoopConfiguration()
    hadoopConf.set('fs.s3a.access.key', access_key)
    hadoopConf.set('fs.s3a.secret.key', secret_key)
    hadoopConf.set('spark.hadoop.fs.s3a.aws.credentials.provider', 'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider')


    df = spark.read.json("s3a://pinterest-data-6caaf6b1-2aef-4376-93c5-e713a3717d92/2022-08-19/1.json")
    #df.printSchema()
    df.show()

    spark.stop()