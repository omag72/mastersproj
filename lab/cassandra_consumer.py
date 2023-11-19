import os
import sys
import json


from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from pyspark import SparkContext

sc = SparkContext()
sc.setLogLevel("WARN")

# Initialize Spark session

spark = SparkSession.builder \
    .appName("KafkaToCassandra") \
    .config("spark.cassandra.connection.host", "cassandra") \
    .config("spark.cassandra.connection.port", "9042") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2,com.datastax.spark:spark-cassandra-connector_2.12:3.1.0,mysql:mysql-connector-java:8.0.26") \
    .getOrCreate()


# Kafka connection parameters
kafka_bootstrap_servers = "broker:29092"
kafka_topic = "Sales"

# Cassandra keyspace and table
cassandra_keyspace = "car_parts"
cassandra_table = "sales_data"

# Define the schema for the incoming JSON data
schema = StructType([
    StructField("transaction_id", StringType(), True),
    StructField("product_name", StringType(), True),
    StructField("category", StringType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("unit_price", DoubleType(), True),
    StructField("timestamp", IntegerType(), True),
    StructField("day_of_week", StringType(), True),
    StructField("month_of_year", IntegerType(), True),
])

# Create a streaming DataFrame that represents data received from Kafka
kafka_df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("subscribe", kafka_topic) \
    .load()

# Deserialize JSON data from Kafka
value_col = from_json(col("value").cast("string"), schema).alias("data")
kafka_df = kafka_df.select(value_col)

# Flatten the nested structure
flattened_df = kafka_df.select("data.*")

# Write the data to Cassandra
query = flattened_df \
    .writeStream \
    .outputMode("append") \
    .foreachBatch(lambda df, epochId: df.write \
        .format("org.apache.spark.sql.cassandra") \
        .option("keyspace",cassandra_keyspace) \
        .option("table",cassandra_table) \
        .mode("append") \
        .save())

# Start the query
query.start().awaitTermination()
