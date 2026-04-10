from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    'banking_server.public.transactions',
    bootstrap_servers='localhost:29092',
    auto_offset_reset='earliest',
    group_id='test-debug-123',
    value_deserializer=lambda x: json.loads(x.decode('utf-8')),
    consumer_timeout_ms=10000
)

print("Listening...")
count = 0
for msg in consumer:
    count += 1
    print(f"Got message {count}: {msg.topic}")
print(f"Total: {count}")