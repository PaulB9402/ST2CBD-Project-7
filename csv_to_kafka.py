import json
import pandas as pd
from confluent_kafka import Producer
import tqdm

# Kafka Configuration
conf = {
    'bootstrap.servers': '172.19.0.3:9092',
    'client.id': 'my-producer'
}
producer = Producer(conf)
topic_name = 'transactions'

# CSV Dataset Path (replace with your actual path)
file_path = r'C:\Users\super\Documents\GitHub\ST2CBD-Project-7\transactions.csv'

# Batch Size and Row Limit
batch_size = 100
row_limit = 10000


# Callback function for delivery reports
def delivery_report(err, msg):
    if err is not None:
        print(f'Message delivery failed: {err}')
    else:
        print(f'Message delivered to {msg.topic()} [{msg.partition()}]')


# Read CSV in chunks (limiting to 10000 rows)
total_rows_sent = 0
chunk_count = 0
for chunk in pd.read_csv(file_path, chunksize=batch_size, parse_dates=['Date']):
    chunk_count += 1

    # Limit the chunk to the remaining rows needed
    if total_rows_sent + len(chunk) > row_limit:
        chunk = chunk.iloc[:row_limit - total_rows_sent]

    with tqdm.tqdm(total=len(chunk), desc=f"Processing Chunk {chunk_count}") as pbar:
        for _, row in chunk.iterrows():
            row_dict = row.to_dict()

            if isinstance(row_dict['Date'], pd.Timestamp):
                row_dict['Date'] = row_dict['Date'].isoformat()
            else:
                print(f"Warning: Date in row {row_dict} is not a datetime object. Skipping conversion.")

            producer.produce(topic_name, value=json.dumps(row_dict), callback=delivery_report)
            pbar.update(1)
            total_rows_sent += 1

    # Poll to ensure messages in the chunk are sent
    producer.poll(1.0)

    # Break if we've reached the row limit
    if total_rows_sent >= row_limit:
        break

print(f"Sent {total_rows_sent} rows of CSV data to Kafka successfully!")