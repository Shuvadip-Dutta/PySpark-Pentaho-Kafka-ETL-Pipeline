import json
import time
import pandas as pd
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda x: json.dumps(x).encode("utf-8")
)

df = pd.read_csv("data/FurnitureSales.csv")

# Convert pandas NaN to Python None
df = df.where(pd.notnull(df), None)

for _, row in df.iterrows():
    producer.send("furniture_sales", row.to_dict())
    print(f"Sent: {row['Salesperson']}")
    time.sleep(1)

producer.flush()
producer.close()