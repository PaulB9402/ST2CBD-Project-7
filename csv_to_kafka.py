import csv
from kafka import KafkaProducer


def send_transactions_to_kafka(csv_file_path):
    producer = KafkaProducer(bootstrap_servers='localhost:9092')
    with open(csv_file_path, 'r') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            transaction_data = {
                'TransactionNo': int(row['TransactionNo']),
                'Date': row['Date'],
                'ProductNo': int(row['ProductNo']),
                'ProductName': row['ProductName'],
                'Price': float(row['Price']),
                'Quantity': int(row['Quantity']),
                'CustomerNo': int(row['CustomerNo']),
                'Country': row['Country']
            }
            producer.send('transactions', value=str(transaction_data).encode('utf-8'))
            producer.flush()


if __name__ == '__main__':
    csv_file_path = 'transactions.csv'  # Replace with your CSV file path
    send_transactions_to_kafka(csv_file_path)
