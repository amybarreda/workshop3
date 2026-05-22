from sqlalchemy import create_engine, text

DB_URI = "postgresql://user:password@localhost:5432/streaming_etl"

def reset_database():
    try:
        engine = create_engine(DB_URI)
        with engine.connect() as conn:
            print("Dropping old tables...")
            conn.execute(text("DROP TABLE IF EXISTS fact_predictions, dim_country, dim_date, dim_raw_event, dim_model, raw_happiness_events CASCADE"))
            conn.commit()
            print("Database successfully reset!")
    except Exception as e:
        print(f"Error resetting database: {e}")

if __name__ == "__main__":
    reset_database()
