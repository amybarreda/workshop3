# Streaming ETL Pipeline: Happiness Score Predictor

## 1. Project Description
This project implements an end-to-end Streaming ETL Pipeline to predict World Happiness Scores based on socio-economic indicators. It leverages historical CSV data (2015-2019) to train a machine learning model, which is then deployed into a real-time streaming architecture using Apache Kafka and PostgreSQL. The goal of the project is to build a robust, fault-tolerant data pipeline rather than maximizing model accuracy.

## 2. Architecture Explanation
The architecture is divided into three parts:
- **Part A (Offline Processing):** We perform Exploratory Data Analysis (EDA) on raw CSV files, harmonize their schemas, and train a `RandomForestRegressor`. The model is serialized as `model.pkl`.
- **Part B (Streaming ETL):** A Kafka Producer streams the cleaned data row-by-row into a Kafka topic (`happiness-predictions`). A Kafka Consumer listens to this topic, validates the incoming JSON schema in real-time, generates a prediction using the serialized model, and stores the results.
- **Part C (Storage & Analytics):** A PostgreSQL database stores the raw JSON events (for audit purposes) and an advanced **MLOps Star Schema** containing dimensions and prediction results, enabling real-time BI dashboards.

## 3. Data Cleaning Decisions
During the EDA phase, we identified that the 2015-2019 datasets had inconsistent column names (e.g., `Economy (GDP per Capita)` vs `GDP per capita`). 
- **Schema Harmonization:** All columns were mapped to a standard schema: `country`, `happiness_score`, `gdp`, `family`, `health`, `freedom`, `corruption`, `generosity`, and `year`.
- **Missing Values:** A single missing value was found in the 2018 `corruption` column and was imputed using the median corruption score of that year to preserve data integrity.

## 4. Feature Engineering Decisions
For the ML training phase, we strictly selected socio-economic indicators as features (`gdp`, `family`, `health`, `freedom`, `corruption`, `generosity`) and defined `happiness_score` as the target.
- **Dropped `country`:** Country is a high-cardinality categorical variable. Including it would cause severe overfitting, making the model act like a lookup table rather than learning the underlying socio-economic patterns.
- **Dropped `year`:** Time was removed to prevent target leakage and ensure the model generalizes effectively to future real-time streaming events.
- **Scaling:** A `StandardScaler` was bundled with the `RandomForestRegressor` inside a scikit-learn `Pipeline`. This ensures the exact same transformation logic is applied consistently during both training and real-time streaming inference.

## 5. Kafka Pipeline Explanation
- **Producer:** Simulates a real-time stream by reading the harmonized historical data and pushing each row as a JSON payload to Kafka.
- **Consumer Validation:** The consumer is fault-tolerant. It saves every raw message to a PostgreSQL table `raw_happiness_events` with a `PENDING` status. It then rigorously checks for missing fields or negative values. If validation fails, it marks the DB record as `INVALID_SCHEMA` and skips prediction without crashing the pipeline.

## 6. Database Design (MLOps Star Schema)
The analytical database (PostgreSQL) implements a highly robust Star Schema, fulfilling and exceeding standard requirements by adding an MLOps dimension:
- **`raw_happiness_events`**: The audit table holding the exact JSON payload and its processing status.
- **`dim_country`**: Dimension table storing country names.
- **`dim_date`**: Dimension table storing the year.
- **`dim_raw_event`**: Dimension table that strictly structures the numerical features (`gdp`, `family`, `health`, etc.) extracted from the valid JSON payload.
- **`dim_model` (Justified Addition)**: MLOps dimension storing the exact Model Name and Version that generated the prediction, enabling precise model tracking over time.
- **`fact_predictions`**: The central fact table linking all dimensions. It stores the `actual_score`, `predicted_score`, and the calculated `prediction_error`.

## 7. Dashboard Explanation
A Python-based **Streamlit + Plotly** dashboard (`dashboards/app.py`) executes SQL views (`sql/kpis.sql`) directly against PostgreSQL. It features a Premium Deep Navy theme and interactive charts.

**KPI Explanations:**
1. **Average Prediction Error (MAE):** The global Mean Absolute Error across all predictions. A lower number indicates the model is highly accurate in real-time inference.
2. **Records Status (Donut Chart):** Tracks the validation state of the Kafka stream (`VALID`, `INVALID_SCHEMA`, `INVALID_VALUES`). Critical for monitoring data pipeline health.
3. **Features Drift Over Time:** Tracks the evolution of the independent variables (`gdp`, `family`, `health`, `freedom`) across years. Essential for detecting if the underlying data distribution is changing, which might trigger a model retraining.
4. **Predictions by Country (Combo Chart):** A bar chart of the actual scores overlayed with a trend line for predicted scores. Identifies geographical bias in the model.
5. **Happiness Score Trends:** A line chart comparing the global average actual score vs the predicted score over time. Verifies if the model captures temporal macro-trends despite `year` being excluded from training.
6. **Predicted vs Actual (Scatter Plot):** Assesses the variance and heteroscedasticity of the model's errors. A perfect model would form a tight diagonal line.

*Note: Screenshots of the functioning dashboard are located in the `images/` directory.*

## 8. Execution Instructions

**Prerequisites:**
- Docker and Docker Compose
- Python 3.12+ (or `uv` package manager)

**1. Start Infrastructure:**
```bash
docker-compose up -d
```
This will start Zookeeper, Kafka, and PostgreSQL.

**2. Setup Environment:**
```bash
uv venv
uv pip install -r requirements.txt
uv pip install streamlit
```

**3. Run the Streaming Pipeline:**
Open two terminal windows.
In Terminal 1, start the Consumer (it will initialize the DB schema automatically):
```bash
uv run python kafka/consumer.py
```
In Terminal 2, start the Producer to send the data:
```bash
uv run python kafka/producer.py
```

**4. View Dashboards:**
In a third terminal window, start the Streamlit Dashboard:
```bash
uv run streamlit run dashboards/app.py
```
Open your browser at `http://localhost:8501` to view the live KPIs.
