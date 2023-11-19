from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

# Initialize Spark session
spark = SparkSession.builder \
    .appName("CassandraToMySQL") \
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
    .options(keyspace=cassandra_keyspace, table=cassandra_table) \
    .load()

# Add a total_price column
cassandra_df = cassandra_df.withColumn("total_price", col("unit_price") * col("quantity"))

# Write data to MySQL
cassandra_df \
    .write \
    .mode("append") \
    .jdbc(mysql_url, "sales_data", properties=mysql_properties)
    
