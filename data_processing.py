from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, sum, avg, max, min, count
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType, DateType
from pyspark.sql import DataFrame

# PostgreSQL Configuration
POSTGRES_URL = "jdbc:postgresql://localhost:5432/ST2CBD"
POSTGRES_USER = "postgres"
POSTGRES_PASSWORD = "1234"
POSTGRES_TABLE = "transactions_aggregated"

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("KafkaSparkStreaming") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.1,org.postgresql:postgresql:42.2.23") \
    .getOrCreate()

# Define Schema
schema = StructType([
    StructField("TransactionNo", IntegerType(), True),
    StructField("Date", DateType(), True),
    StructField("ProductNo", IntegerType(), True),
    StructField("ProductName", StringType(), True),
    StructField("Price", FloatType(), True),
    StructField("Quantity", IntegerType(), True),
    StructField("CustomerNo", IntegerType(), True),
    StructField("Country", StringType(), True)
])

# Read from Kafka
kafka_df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "transactions") \
    .load()

# Convert Kafka value to String and parse JSON
transactions_df = kafka_df.selectExpr("CAST(value AS STRING) as json") \
    .select(from_json(col("json"), schema).alias("data")).select("data.*")

# Print schema for verification
transactions_df.printSchema()

# Transformation Logic
transformed_df = transactions_df \
    .withColumn("timestamp", col("Date").cast("timestamp")) \
    .groupBy(window("timestamp", "1 day"), "Country", "ProductName") \
    .agg(
        sum("Price").alias("total_revenue"),
        avg("Price").alias("average_price"),
        max("Price").alias("max_price"),
        min("Price").alias("min_price"),
        count("TransactionNo").alias("transaction_count")
    )

# Write to PostgreSQL
def write_to_postgres(df: DataFrame, epoch_id):
    try:
        df.write.format("jdbc") \
            .option("url", POSTGRES_URL) \
            .option("dbtable", POSTGRES_TABLE) \
            .option("user", POSTGRES_USER) \
            .option("password", POSTGRES_PASSWORD) \
            .mode("append") \
            .save()
        print("Inserted data into PostgreSQL successfully!")
    except Exception as e:
        print(f"Error writing to PostgreSQL: {e}")

# Start the Streaming Query
query = transformed_df.writeStream \
    .foreachBatch(write_to_postgres) \
    .outputMode("update") \
    .option("checkpointLocation", "/tmp/kafka-spark-checkpoints") \
    .start()

query.awaitTermination()
