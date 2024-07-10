import json
import psycopg2
from confluent_kafka import Consumer, KafkaError
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer

# PostgreSQL Configuration
POSTGRES_URL = "jdbc:postgresql://localhost:5432/ST2CBD"
POSTGRES_USER = "postgres"
POSTGRES_PASSWORD = "1234"
POSTGRES_TABLE = "transactions_aggregated"

# Kafka Configuration
conf = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'my-consumer-group',
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': False
}

# Schema Registry Configuration (Optional)
schema_registry_conf = {
    'url': 'http://localhost:8081'  # Replace with your schema registry URL if using one
}
schema_registry_client = SchemaRegistryClient(schema_registry_conf)

# Deserializer (Optional)
value_deserializer = AvroDeserializer(schema_registry_client)

# Consumer
consumer = Consumer(conf)
consumer.subscribe(['transactions'])

# PostgreSQL Connection
conn = psycopg2.connect(
    dbname="ST2CBD",
    user=POSTGRES_USER,
    password=POSTGRES_PASSWORD,
    host="localhost",
    port="5432"
)
cur = conn.cursor()


def process_message(msg):
    # Parse message value
    try:
        value = json.loads(msg.value())
    except json.JSONDecodeError as e:
        print(f"Error decoding JSON: {e}")
        return

    # Extract data from the message
    transaction_no = value.get('TransactionNo')
    product_no = value.get('ProductNo')
    product_name = value.get('ProductName')
    price = value.get('Price')
    quantity = value.get('Quantity')
    customer_no = value.get('CustomerNo')
    country = value.get('Country')
    date = value.get('Date')

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

    # Execute INSERT (including quantity)
    cur.execute(sql, (transaction_no, product_no, product_name, price, quantity, customer_no, country, date))
    conn.commit()


while True:
    try:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            print(f"Consumer error: {msg.error()}")
            continue

        # Process each message
        process_message(msg)

    except KeyboardInterrupt:
        break

# Close consumer and database connection
consumer.close()
cur.close()
conn.close()

print("Stopped consuming from Kafka.")
