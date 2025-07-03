from pyspark.sql import SparkSession
from pyspark.sql.functions import sum as _sum, col, udf
from pyspark.sql.types import FloatType
from textblob import TextBlob

def sentiment(text):
    return TextBlob(text).sentiment.polarity

spark = SparkSession.builder.appName("YTBatch").getOrCreate()

df = spark.read.json("s3a://nci-cloud-project/medium_dataset.json")

wc = df.groupBy("video_id").agg(
    _sum("views").alias("total_views"),
    _sum("likes").alias("total_likes")
)

sentiment_udf = udf(sentiment, FloatType())
df2 = df.withColumn("sentiment", sentiment_udf(col("description")))
sentiment_agg = df2.groupBy("video_id").agg({"sentiment": "avg"})

final_df = wc.join(sentiment_agg, "video_id")
final_df.write.csv("s3a://nci-cloud-project/output/batch_results/", header=True)
