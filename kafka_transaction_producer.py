import json
import random
import uuid
from datetime import datetime, timedelta
from time import sleep

from faker import Faker
from kafka import KafkaProducer

fake = Faker()

# Config
TOPIC = "transactions"
BROKER = "kafka:9092"  # Adjust for your Kafka broker

# Setup producer
producer = KafkaProducer(
    bootstrap_servers=BROKER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

# Sample values
categories = {
    "Walmart": "Grocery",
    "Shell": "Fuel",
    "Amazon": "Shopping",
    "BestBuy": "Electronics",
    "Starbucks": "Coffee",
    "Target": "Grocery",
    "Delta": "Travel",
    "Uber": "Transport"
}
merchants = list(categories.keys())
locations = ["New York", "Los Angeles", "Chicago", "Houston", "Phoenix"]

def generate_transaction(customer_id: str, txn_count: int):
    merchant = random.choice(merchants)
    return {
        "transaction_id": f"txn{str(txn_count).zfill(5)}",
        "customer_id": customer_id,
        "amount": round(random.uniform(5.0, 500.0), 2),
        "merchant": merchant,
        "category": categories[merchant],
        "transaction_time": (datetime.now() - timedelta(days=random.randint(0, 60))).isoformat(),
        "location": random.choice(locations)
    }

# Generate data for 100 customers (e.g., cust001 to cust100)
customer_ids = [f"cust{str(i).zfill(3)}" for i in range(1, 101)]

print("Producing Kafka messages...")

txn_counter = 1
for _ in range(500):  # Send 500 transactions total
    cust_id = random.choice(customer_ids)
    txn = generate_transaction(cust_id, txn_counter)
    print(txn)
    producer.send(TOPIC, txn)
    txn_counter += 1
    print(f"Sent: {txn['transaction_id']} | Customer: {txn['customer_id']}")
    sleep(10)  # Slow down for visibility (optional)

producer.flush()
producer.close()

print("✅ All messages sent.")

# pip install kafka-python faker