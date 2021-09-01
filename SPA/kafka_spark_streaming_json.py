from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

import time

kafka_topic_name = "Post"
kafka_bootstrap_servers = 'localhost:9092'

if __name__ == "__main__":
    print("Welcome to DataMaking !!!")
    print("Stream Data Processing Application Started ...")
    print(time.strftime("%Y-%m-%d %H:%M:%S"))

    spark = SparkSession \
        .builder \
        .appName("PySpark Structured Streaming with Kafka and Message Format as JSON") \
        .master("local[*]") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    # Construct a streaming DataFrame that reads from post topic.
    message_df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
        .option("subscribe", kafka_topic_name) \
        .option("startingOffsets", "latest") \
        .load()

    print("Printing Schema of message_df: ")
    message_df.printSchema()

    message_df1 = message_df.selectExpr("CAST(value AS STRING)", "timestamp")

    # Define a schema for the message data

    message_schema = StructType() \
        .add("action_id", IntegerType()) \
        .add("action_name", StringType()) \
        .add("action_reference_id", IntegerType()) \
        .add("user_id", StringType()) \
        .add("user_name", StringType()) \
        .add("group_id", IntegerType()) \
        .add("group_name", StringType()) \
        .add("group_description", StringType()) \
        .add("community_id", IntegerType()) \
        .add("communtiy_name", StringType()) \
        .add("community_description", StringType()) \
        .add("action_data_id", StringType()) \
        .add("action_description", StringType()) \
        .add("group_id", StringType()) \
        .add("action_data", StringType()) \
        .add("timestamp", StringType())

    message_df2 = message_df1\
        .select(from_json(col("value"), message_schema)\
        .alias("message"), "timestamp")

    message_df3 = message_df2.select("message.*", "timestamp")

    message_df4 =  message_df3.select("action_id", "action_name", "user_id", "user_name", "action_data") \
        .alias("timestamp")

    print("Printing Schema of message_df4: ")
    message_df4.printSchema()

    # Write final result into console for debugging purpose
    message_agg_write_stream = message_df4 \
        .writeStream \
        .trigger(processingTime='5 seconds') \
        .outputMode("update") \
        .option("truncate", "false")\
        .format("console") \
        .start()

    message_agg_write_stream.awaitTermination()

    print("Stream Data Processing Application Completed.")