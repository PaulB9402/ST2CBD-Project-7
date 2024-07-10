import os
import threading
import time
import json

from dotenv import load_dotenv
from psycopg2 import sql
from kafka import KafkaConsumer

from database import DatabaseHandler

# Load environment variables from the .env file
load_dotenv()

# Get environment variables with defaults
dbname = os.getenv('POSTGRES_DB', 'ST2CBD')
user = os.getenv('POSTGRES_USER', 'postgres')
password = os.getenv('POSTGRES_PASSWORD', '1234')
host = os.getenv('POSTGRES_HOST', 'localhost')
port = int(os.getenv('POSTGRES_PORT', 5432))

# Print the environment variables to debug
print(f"POSTGRES_DB: {dbname}")
print(f"POSTGRES_USER: {user}")
print(f"POSTGRES_PASSWORD: {password}")
print(f"POSTGRES_HOST: {host}")
print(f"POSTGRES_PORT: {port}")

consumer = KafkaConsumer(
    'transactions',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='transactions-consumers',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

# Initialize DatabaseHandler with environment variables
db_handler = DatabaseHandler(
    dbname=dbname,
    user=user,
    password=password,
    host=host,
    port=port
)

# Dictionary to store transaction statistics
transaction_stats = {}

def process_transaction(transaction, db_handler):
    try:
        product_no = transaction["ProductNo"]
        if product_no not in transaction_stats:
            transaction_stats[product_no] = {"count": 0, "total_amount": 0}

        transaction_stats[product_no]["count"] += 1
        transaction_stats[product_no]["total_amount"] += float(transaction["Amount"])

        query = sql.SQL(
            "INSERT INTO transactions (transaction_id, date, customer, product_no, amount) VALUES (%s, %s, %s, %s, %s)"
        )
        values = (
            transaction["TransactionID"], transaction["Date"], transaction["Customer"],
            transaction["ProductNo"], transaction["Amount"]
        )
        db_handler.execute_query(query, values)
    except Exception as e:
        print(f"Error processing transaction: {e}")

def display_stats():
    while True:
        print("\nCurrent Transaction Stats:")
        for product_no, stats in transaction_stats.items():
            print(f"Product {product_no}:")
            print(f"Number of Transactions: {stats['count']}")
            print(f"Total Amount: {stats['total_amount']}")
        time.sleep(10)

display_thread = threading.Thread(target=display_stats)
display_thread.start()

print("Starting Consumer...")

for message in consumer:
    transaction = message.value
    print(f"Received transaction: {transaction}")
    process_transaction(transaction, db_handler)
