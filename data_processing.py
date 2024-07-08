from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, FloatType, DateType, IntegerType
from pymongo import MongoClient

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


# Process the transactions data (transformation logic goes here)

# Write to MongoDB
def write_to_mongo(df, epoch_id):
    client = MongoClient("mongodb://localhost:27017/")
    db = client["natone_db"]
    collection = db["transactions"]
    data = df.toPandas().to_dict(orient='records')
    if data:
        collection.insert_many(data)


transactions_df.writeStream.foreachBatch(write_to_mongo).start().awaitTermination()
