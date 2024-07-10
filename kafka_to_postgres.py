from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType
import psycopg2
import logging
import sys

# Configure logging
logging.basicConfig(stream=sys.stdout, level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize Spark Session with Kafka package
try:
    spark = SparkSession.builder \
        .appName("KafkaSparkStreaming") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2") \
        .config("spark.hadoop.fs.defaultFS", "hdfs://localhost:9000") \
        .config("spark.sql.warehouse.dir", "hdfs://localhost:9000/user/spark/warehouse") \
        .getOrCreate()
    logger.info("Spark Session created successfully")
except Exception as e:
    logger.error(f"Error creating Spark Session: {e}")
    sys.exit(1)

# PostgreSQL Configuration
POSTGRES_URL = "jdbc:postgresql://localhost:5432/ST2CBD"
POSTGRES_USER = "postgres"
POSTGRES_PASSWORD = "1234"
POSTGRES_TABLE = "transactions_aggregated"

# Define the schema of the JSON data
schema = StructType([
    StructField("TransactionNo", StringType(), True),
    StructField("ProductNo", StringType(), True),
    StructField("ProductName", StringType(), True),
    StructField("Price", FloatType(), True),
    StructField("Quantity", IntegerType(), True),
    StructField("CustomerNo", StringType(), True),
    StructField("Country", StringType(), True),
    StructField("Date", StringType(), True)
])

# Read data from Kafka
try:
    df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "transactions") \
        .option("startingOffsets", "earliest") \
        .load()
    logger.info("Kafka stream loaded successfully")
except Exception as e:
    logger.error(f"Error loading Kafka stream: {e}")
    sys.exit(1)

# Deserialize JSON data
df = df.selectExpr("CAST(value AS STRING) as json")
df = df.select(from_json(col("json"), schema).alias("data")).select("data.*")

def foreach_batch_function(df, epoch_id):
    try:
        pandas_df = df.toPandas()
        logger.info(f"Processing batch with {len(pandas_df)} records")

        conn = psycopg2.connect(
            dbname="ST2CBD",
            user=POSTGRES_USER,
            password=POSTGRES_PASSWORD,
            host="localhost",
            port="5432"
        )
        cur = conn.cursor()

        for _, row in pandas_df.iterrows():
            sql = """
            INSERT INTO transactions_aggregated (TransactionNo, ProductNo, ProductName, Price, Quantity, CustomerNo, Country, Date) 
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s) 
            ON CONFLICT (TransactionNo) DO UPDATE 
            SET ProductName = EXCLUDED.ProductName,
                Price = EXCLUDED.Price,
                Quantity = EXCLUDED.Quantity,
                CustomerNo = EXCLUDED.CustomerNo,
                Country = EXCLUDED.Country,
                Date = EXCLUDED.Date;
            """
            cur.execute(sql, (
                row['TransactionNo'], row['ProductNo'], row['ProductName'],
                row['Price'], row['Quantity'], row['CustomerNo'],
                row['Country'], row['Date']
            ))
        conn.commit()
        cur.close()
        conn.close()
        logger.info("Batch processed successfully")
    except Exception as e:
        logger.error(f"Error processing batch: {e}")

# Write stream to PostgreSQL
try:
    query = df.writeStream \
        .foreachBatch(foreach_batch_function) \
        .outputMode("update") \
        .option("checkpointLocation", "hdfs://localhost:9000/user/spark/checkpoints") \
        .start()
    logger.info("Streaming query started successfully")
    query.awaitTermination()
except Exception as e:
    logger.error(f"Error starting streaming query: {e}")
    sys.exit(1)
