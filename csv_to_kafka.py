import json
import pandas as pd
from kafka import KafkaProducer

# Kafka Configuration
producer = KafkaProducer(bootstrap_servers='localhost:9092')
topic_name = 'raw_csv_data'

# CSV Dataset Path (replace with your actual path)
file_path = r'C:\Users\drnru\Downloads\archive\my_e_commerce_data_set.csv'

# Read CSV into Pandas DataFrame
df = pd.read_csv(file_path)

# Convert Date column to datetime if it's not already
df['Date'] = pd.to_datetime(df['Date'])

# Iterate through rows and send to Kafka
for _, row in df.iterrows():
    # Convert row to dictionary and then to JSON
    row_dict = row.to_dict()

    # Convert datetime to ISO format for compatibility
    row_dict['Date'] = row_dict['Date'].isoformat()

    # Convert dictionary to JSON string and send to Kafka
    producer.send(topic_name, json.dumps(row_dict).encode('utf-8'))

# Ensure all messages are sent
producer.flush()

print("CSV data sent to Kafka successfully!")
