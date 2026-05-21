-- Table for storing the raw streaming events from Kafka
CREATE TABLE IF NOT EXISTS raw_happiness_events (
    id SERIAL PRIMARY KEY,
    event_json JSONB NOT NULL,
    processing_status VARCHAR(50) DEFAULT 'PENDING',
    ingestion_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Dimension table for countries
CREATE TABLE IF NOT EXISTS dim_country (
    country_id SERIAL PRIMARY KEY,
    country_name VARCHAR(255) UNIQUE NOT NULL
);

-- Dimension table for dates/years
CREATE TABLE IF NOT EXISTS dim_date (
    date_id SERIAL PRIMARY KEY,
    year INTEGER UNIQUE NOT NULL
);

-- Fact table for predictions
CREATE TABLE IF NOT EXISTS fact_predictions (
    prediction_id SERIAL PRIMARY KEY,
    raw_event_id INTEGER REFERENCES raw_happiness_events(id),
    country_id INTEGER REFERENCES dim_country(country_id),
    date_id INTEGER REFERENCES dim_date(date_id),
    actual_score NUMERIC,
    predicted_score NUMERIC,
    prediction_error NUMERIC,
    prediction_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
