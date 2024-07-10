import json
import logging
import pandas as pd
import time
from kafka import KafkaProducer

# Kafka Configuration
producer = KafkaProducer(bootstrap_servers='localhost:9092')
topic_name = 'transactions'

# CSV Dataset Path (replace with your actual path)
file_path = r'C:\Users\drnru\Downloads\archive\my_e_commerce_data_set.csv'

# Batch Size and Other Parameters
batch_size = 1000
sleep_interval = 1
row_limit = 100000
# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Read CSV in chunks
for batch_num, chunk in enumerate(pd.read_csv(file_path, chunksize=batch_size, parse_dates=['Date']), 1):

    # Preprocess data: convert Date and ProductNo
    chunk['Date'] = pd.to_datetime(chunk['Date'])
    chunk['ProductNo'] = chunk['ProductNo'].astype(str)

    # Log before sending the batch
    logging.info(f"Sending batch {batch_num} with {len(chunk)} rows to Kafka...")

    for _, row in chunk.iterrows():
        row_dict = row.to_dict()
        row_dict['Date'] = row_dict['Date'].isoformat()
        producer.send(topic_name, json.dumps(row_dict).encode('utf-8'))

    producer.flush()  # Ensure messages are sent before sleeping

    # Log after sending the batch
    logging.info(f"Batch {batch_num} sent successfully.")

    time.sleep(sleep_interval)

print("CSV data sent to Kafka successfully!")
