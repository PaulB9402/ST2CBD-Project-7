from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, sum, avg, max, min, count
from pyspark.sql.types import *
from pyspark.sql import DataFrame

# PostgreSQL Configuration
POSTGRES_URL = "jdbc:postgresql://localhost:5432/ST2CBD"
POSTGRES_USER = "postgres"
POSTGRES_PASSWORD = "{Pa42!Bo86"
POSTGRES_TABLE = "transactions_aggregated"

spark = SparkSession.builder.appName("KafkaSparkStreaming").getOrCreate()

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

kafka_df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "transactions") \
    .load()

transactions_df = kafka_df.selectExpr("CAST(value AS STRING) as json").select(
    from_json(col("json"), schema).alias("data")).select("data.*")

'''
# Transformation Logic
transformed_df = transactions_df \
    .withColumn("timestamp", (col("Date").cast("timestamp").alias("timestamp"))) \
    .groupBy(window("timestamp", "1 day"), "Country", "ProductName") \
    .agg(
    sum("Price").alias("total_revenue"),
    avg("Price").alias("average_price"),
    max("Price").alias("max_price"),
    min("Price").alias("min_price"),
    count("TransactionNo").alias("transaction_count")
)
'''


# Write to PostgreSQL (with error handling and status logging)
def write_to_postgres(df: DataFrame, epoch_id):
    try:
        df.write.format("jdbc") \
            .option("url", POSTGRES_URL) \
            .option("dbtable", POSTGRES_TABLE) \
            .option("user", POSTGRES_USER) \
            .option("password", POSTGRES_PASSWORD) \
            .mode("append") \
            .save()
        print(f"Inserted data into PostgreSQL successfully!")
    except Exception as e:
        print(f"Error writing to PostgreSQL: {e}")


# Start the Streaming Query
'''transformed_df'''
transactions_df.writeStream \
    .foreachBatch(write_to_postgres) \
    .outputMode("update") \
    .start() \
    .awaitTermination()
