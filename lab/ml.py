

from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import RandomForestRegressor
from pyspark.ml import Pipeline
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
import numpy as np

# Initialize Spark session
spark = SparkSession.builder \
    .appName("CassandraToMySQLML") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2,com.datastax.spark:spark-cassandra-connector_2.12:3.1.0,mysql:mysql-connector-java:8.0.26") \
    .getOrCreate()

# Cassandra keyspace and table
cassandra_keyspace = "car_parts"
cassandra_table = "sales_data"

# MySQL connection properties
mysql_url = "jdbc:mysql://mysql:3306/mydatabase"
mysql_properties = {
    "driver": "com.mysql.cj.jdbc.Driver",
    "user": "myuser",
    "password": "mypassword",
}

# Define the schema for the Cassandra data
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

# Read data from Cassandra
cassandra_df = spark \
    .read \
    .format("org.apache.spark.sql.cassandra") \
    .options(keyspace=cassandra_keyspace, table=cassandra_table, schema=schema) \
    .load()

# Add a total_price column
cassandra_df = cassandra_df.withColumn("total_price", col("unit_price") * col("quantity"))

# Write data to MySQL
cassandra_df \
    .write \
    .mode("overwrite") \
    .jdbc(mysql_url, "sales_data", properties=mysql_properties)

# Read data from MySQL
mysql_df = spark \
    .read \
    .jdbc(mysql_url, "sales_data", properties=mysql_properties)

# Feature engineering
assembler = VectorAssembler(inputCols=["quantity", "unit_price", "total_price"], outputCol="features")
dataset = assembler.transform(mysql_df)

# Split the data into training and testing sets
(train_data, test_data) = dataset.randomSplit([0.8, 0.2], seed=123)

# Define the machine learning model
rf = RandomForestRegressor(featuresCol="features", labelCol="month_of_year")

# Create a pipeline for the model
pipeline = Pipeline(stages=[assembler, rf])

# Train the model
model = pipeline.fit(train_data)

# Make predictions on the testing set
predictions = model.transform(test_data)

# Evaluate the model
evaluator = RegressionEvaluator(labelCol="month_of_year", predictionCol="prediction", metricName="rmse")
rmse = evaluator.evaluate(predictions)

print(f"Root Mean Squared Error (RMSE) on test data: {rmse}")

# Use the model to make predictions on new data (adjust 'new_data' accordingly)
new_data = ...  # Prepare new data for prediction
new_predictions = model.transform(new_data)

# Show the predictions on new data
new_predictions.select("transaction_id", "prediction").show()

# Stop the Spark session
spark.stop()