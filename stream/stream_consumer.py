from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType, IntegerType, TimestampType

# Define schema
schema = StructType() \
    .add("video_id", StringType()) \
    .add("timestamp", TimestampType()) \
    .add("views", IntegerType()) \
    .add("likes", IntegerType()) \
    .add("subscribers", IntegerType()) \
    .add("description", StringType())

# Start Spark session
spark = SparkSession.builder \
    .appName("YTStreaming") \
    .config("spark.hadoop.fs.s3a.access.key", "ASIA5KQVF27MFUMTB6F5")\
    .config("spark.hadoop.fs.s3a.secret.key", "LKTU3eUpHPX3j5IPzmqHgxfFAWZtqn8jmZfB/XIk")\
    .config("spark.hadoop.fs.s3a.session.token", "IQoJb3JpZ2luX2VjEAYaCXVzLXdlc3QtMiJIMEYCIQDT1IjRruvLevycgUG+p9ajMnJLO57k4b7nXy8rII3krgIhAPDk4Bezc/77N4NZEpI5mgaVXFdEXqhlnbd1o3CQkhBaKr0CCP///////////wEQABoMOTE1OTQ2MTk5MDAwIgxp2yCRIk7K7V4aSDIqkQJx1uTS/0nl8lTtcPuOLReaNFa0lwLHXAw2wBtwzsjQW5S2gtDOckDCmfyC/hSQ/V4Wq3XiR5bN6vXp3C6W4ceaVK3z+yxx0ofXGmFf/2LaMQHyfbGXJWbUZMCdSVjg8WDDDc4sjPaWXR/xT0tU7DZKtmun47taAMGbchhzT35AM+oVIzxYsfPAWDuDDratzVGd4+e2C84ejElLbnuTGPshdHHPRKrb9LCivWlZVyfOPHef7EESyXA2mEIUQouRLmqDGf1goPKkrQXFfBnIAFiSTa1/OBJen87kvT32kUkgeM1MDqZNhnmqD2Yq4IUrwkxNWxJPCOGX5YiPhKJsNSJC8+KbMqsICQ0UZ3fmZDXvT7swhauYwwY6nAFeQXlb+5CO0T13TA3q10RjaMc+2lDOnDVa/yxFKBNdM5t+QQWaLofDUa/+uhuzC3nWuozo27S7XzERcgudVa5cBeI5pnwk7RxLDYVoqik9NXVv/sGXlPMVyGWSNmuUMq/D/ib8nbSA5hrDYVCydfnyDXrXS+E5/7a2mDb0cGK7ZzZtqlqqU9yhjMWUM4Kp90IgE1CAhoQazHqYJcE=")\
    .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Read from Kinesis
df_raw = spark.readStream \
    .format("kinesis") \
    .option("streamName", "youtube-metrics-stream") \
    .option("region", "us-east-1") \
    .option("startingposition", "LATEST") \
    .load()

# Parse JSON
df_parsed = df_raw \
    .selectExpr("CAST(data AS STRING) as json_str") \
    .select(from_json(col("json_str"), schema).alias("data")) \
    .select("data.*")

# Write to S3
query = df_parsed.writeStream \
    .format("parquet") \
    .option("path", "s3a://nci-cloud-project/output/") \
    .option("checkpointLocation", "s3a://nci-cloud-project/output/checkpoints/") \
    .outputMode("append") \
    .trigger(processingTime="10 seconds") \
    .start()

query.awaitTermination()



from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kinesis import KinesisUtils, InitialPositionInStream

import json
import boto3
import time

# Spark setup
sc = SparkContext(appName="YTStreamingApp")
ssc = StreamingContext(sc, 10)  # 10-second batch interval

# Kinesis setup
kinesis_stream_name = "youtube-metrics-stream"
kinesis_endpoint = "https://kinesis.us-east-1.amazonaws.com"
region_name = "us-east-1"

lines = KinesisUtils.createStream(
    ssc,
    appName="YTStreamingApp",
    streamName="youtube-metrics-stream",
    endpointUrl=kinesis_endpoint,
    regionName="us-east-1",
    initialPositionInStream=InitialPositionInStream.LATEST,
    checkpointInterval=10
)

# Process records
def process(record):
    try:
        data = json.loads(record)
        print("Video ID:", data["video_id"], "| Views:", data["views"])
    except Exception as e:
        print("Invalid record:", record)

lines.map(lambda x: x).foreachRDD(lambda rdd: rdd.foreach(process))

ssc.start()
ssc.awaitTermination()
