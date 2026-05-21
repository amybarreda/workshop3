import pandas as pd
import json
import time
import os
from kafka import KafkaProducer

# Configuration
TOPIC = "happiness-predictions"
BOOTSTRAP_SERVERS = "localhost:9092"
CSV_PATH = os.path.join("data", "processed", "unified_happiness.csv")
DELAY_SECONDS = 0.5

def json_serializer(data):
    return json.dumps(data).encode("utf-8")

def stream_data():
    print(f"Starting Kafka Producer. Connecting to {BOOTSTRAP_SERVERS}...")
    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=json_serializer
    )
    
    print(f"Reading data from {CSV_PATH}")
    df = pd.read_csv(CSV_PATH)
    
    # Fill any NaNs with None to ensure proper JSON serialization
    df = df.where(pd.notnull(df), None)
    
    sent_count = 0
    for _, row in df.iterrows():
        # Construct the exact JSON structure required by the workshop
        event = {
            "country": row["country"],
            "year": int(row["year"]) if row["year"] is not None else None,
            "gdp": row["gdp"],
            "family": row["family"],
            "health": row["health"],
            "freedom": row["freedom"],
            "generosity": row["generosity"],
            "corruption": row["corruption"],
            "actual_happiness_score": row["happiness_score"]
        }
        
        # Send event to Kafka
        producer.send(TOPIC, value=event)
        sent_count += 1
        print(f"Sent event {sent_count}: {event['country']} - {event['year']}")
        
        time.sleep(DELAY_SECONDS)
        
    producer.flush()
    producer.close()
    print(f"Finished sending {sent_count} events to '{TOPIC}'.")

if __name__ == "__main__":
    stream_data()
