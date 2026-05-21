import json
import os
import time
import pandas as pd
import joblib
from kafka import KafkaConsumer
from sqlalchemy import create_engine, text

# Configuration
TOPIC = "happiness-predictions"
BOOTSTRAP_SERVERS = "localhost:9092"
DB_URI = "postgresql://user:password@localhost:5432/streaming_etl"
MODEL_PATH = os.path.join("models", "model.pkl")

def init_db(engine):
    sql_path = os.path.join("sql", "create_tables.sql")
    if os.path.exists(sql_path):
        with open(sql_path, 'r') as f:
            sql_script = f.read()
        with engine.connect() as conn:
            for statement in sql_script.split(';'):
                if statement.strip():
                    conn.execute(text(statement))
            conn.commit()
        print("Database initialized successfully.")
    else:
        print("Warning: sql/create_tables.sql not found.")

def validate_event(event):
    """
    Validates the incoming JSON event schema.
    Raises ValueError with specific status codes if invalid.
    """
    required_features = ['country', 'year', 'gdp', 'family', 'health', 'freedom', 'corruption', 'generosity']
    
    # Check for missing fields
    for field in required_features:
        if field not in event or event[field] is None:
            raise ValueError(f"INVALID_SCHEMA: Missing required field '{field}'")
            
    numerical_features = ['gdp', 'family', 'health', 'freedom', 'corruption', 'generosity']
    
    # Check data types and numerical values
    for field in numerical_features:
        val = event[field]
        if not isinstance(val, (int, float)):
            raise ValueError(f"INVALID_SCHEMA: Field '{field}' must be numerical")
        if val < 0:
            raise ValueError(f"INVALID_VALUES: Field '{field}' cannot be negative")

def consume_stream():
    print(f"Loading ML model from {MODEL_PATH}...")
    pipeline = joblib.load(MODEL_PATH)
    
    print(f"Connecting to database...")
    engine = create_engine(DB_URI)
    init_db(engine)
    
    print(f"Connecting to Kafka topic '{TOPIC}' at {BOOTSTRAP_SERVERS}...")
    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='happiness-consumer-group',
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )
    
    print("Consumer is running. Waiting for events...")
    
    with engine.connect() as conn:
        for message in consumer:
            event = message.value
            print(f"\nReceived event: {event.get('country', 'Unknown')} - {event.get('year', 'Unknown')}")
            
            # 1. Store Raw Event (Initial State: PENDING)
            raw_insert = text("""
                INSERT INTO raw_happiness_events (event_json, processing_status) 
                VALUES (:event_json, 'PENDING') RETURNING id
            """)
            result = conn.execute(raw_insert, {"event_json": json.dumps(event)})
            raw_event_id = result.scalar()
            conn.commit()
            
            try:
                # 2. Schema Validation
                validate_event(event)
                
                # Data Preparation
                features = ['gdp', 'family', 'health', 'freedom', 'corruption', 'generosity']
                df_features = pd.DataFrame([event], columns=features)
                
                # 3. Generate Prediction
                predicted_score = float(pipeline.predict(df_features)[0])
                actual_score = event.get('actual_happiness_score')
                actual_score = float(actual_score) if actual_score is not None else None
                prediction_error = float(abs(actual_score - predicted_score)) if actual_score is not None else None
                
                print(f"  > Actual: {actual_score} | Predicted: {predicted_score:.4f} | Error: {prediction_error}")
                
                # 4a. Store Dimension (Country)
                country_insert = text("""
                    INSERT INTO dim_country (country_name) 
                    VALUES (:country_name) 
                    ON CONFLICT (country_name) DO NOTHING
                """)
                conn.execute(country_insert, {"country_name": event['country']})
                conn.commit()
                country_query = text("SELECT country_id FROM dim_country WHERE country_name = :country_name")
                country_id = conn.execute(country_query, {"country_name": event['country']}).scalar()
                
                # 4b. Store Dimension (Date/Year)
                date_insert = text("""
                    INSERT INTO dim_date (year) 
                    VALUES (:year) 
                    ON CONFLICT (year) DO NOTHING
                """)
                conn.execute(date_insert, {"year": event['year']})
                conn.commit()
                date_query = text("SELECT date_id FROM dim_date WHERE year = :year")
                date_id = conn.execute(date_query, {"year": event['year']}).scalar()
                
                # 5. Store Fact (Prediction)
                fact_insert = text("""
                    INSERT INTO fact_predictions (
                        raw_event_id, country_id, date_id, actual_score, predicted_score, prediction_error
                    ) VALUES (
                        :raw_event_id, :country_id, :date_id, :actual_score, :predicted_score, :prediction_error
                    )
                """)
                conn.execute(fact_insert, {
                    "raw_event_id": raw_event_id,
                    "country_id": country_id,
                    "date_id": date_id,
                    "actual_score": actual_score,
                    "predicted_score": predicted_score,
                    "prediction_error": prediction_error
                })
                
                # Mark as VALID
                status_update = text("UPDATE raw_happiness_events SET processing_status = 'VALID' WHERE id = :id")
                conn.execute(status_update, {"id": raw_event_id})
                conn.commit()
                
            except ValueError as ve:
                # Handle Validation Errors
                error_msg = str(ve)
                status = "INVALID_SCHEMA"
                if "INVALID_SCHEMA" in error_msg:
                    status = "INVALID_SCHEMA"
                elif "INVALID_VALUES" in error_msg:
                    status = "INVALID_VALUES"
                    
                print(f"  > Validation Failed: {error_msg}")
                status_update = text("UPDATE raw_happiness_events SET processing_status = :status WHERE id = :id")
                conn.execute(status_update, {"status": status, "id": raw_event_id})
                conn.commit()
                
            except Exception as e:
                # Handle unexpected Prediction Errors (prevents crashing)
                print(f"  > Prediction Error: {e}")
                status_update = text("UPDATE raw_happiness_events SET processing_status = 'PREDICTION_ERROR' WHERE id = :id")
                conn.execute(status_update, {"id": raw_event_id})
                conn.commit()

if __name__ == "__main__":
    consume_stream()
