from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType
import psycopg2

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("KafkaSparkStreaming") \
    .getOrCreate()

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
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "transactions") \
    .option("startingOffsets", "earliest") \
    .load()

# Deserialize JSON data
df = df.selectExpr("CAST(value AS STRING) as json")
df = df.select(from_json(col("json"), schema).alias("data")).select("data.*")


# Function to write each batch to PostgreSQL
def foreach_batch_function(df, epoch_id):
    # Convert DataFrame to Pandas
    pandas_df = df.toPandas()

    # Connect to PostgreSQL
    conn = psycopg2.connect(
        dbname="ST2CBD",
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
        host="localhost",
        port="5432"
    )
    cur = conn.cursor()

    # Insert data into PostgreSQL
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


# Write stream to PostgreSQL
query = df.writeStream \
    .foreachBatch(foreach_batch_function) \
    .outputMode("update") \
    .start()

query.awaitTermination()
