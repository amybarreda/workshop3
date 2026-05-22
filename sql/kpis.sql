-- 1. Average Prediction Error View
CREATE OR REPLACE VIEW vw_avg_prediction_error AS
SELECT 
    AVG(prediction_error) AS avg_error
FROM fact_predictions;

-- 2. Predictions by Country View
CREATE OR REPLACE VIEW vw_predictions_by_country AS
SELECT 
    c.country_name,
    COUNT(f.prediction_id) AS total_predictions,
    AVG(f.actual_score) AS avg_actual_score,
    AVG(f.predicted_score) AS avg_predicted_score,
    AVG(f.prediction_error) AS avg_prediction_error
FROM fact_predictions f
JOIN dim_country c ON f.country_id = c.country_id
GROUP BY c.country_name
ORDER BY avg_prediction_error DESC;

-- 3. Prediction Trends Over Time View
CREATE OR REPLACE VIEW vw_prediction_trends AS
SELECT 
    d.year,
    AVG(f.actual_score) AS avg_actual_score,
    AVG(f.predicted_score) AS avg_predicted_score
FROM fact_predictions f
JOIN dim_date d ON f.date_id = d.date_id
GROUP BY d.year
ORDER BY d.year;

-- 4. Predicted vs Actual Score View
CREATE OR REPLACE VIEW vw_predicted_vs_actual AS
SELECT 
    actual_score,
    predicted_score,
    prediction_error
FROM fact_predictions;

-- 5. Records Processing Status (For Donut Chart)
CREATE OR REPLACE VIEW vw_processing_status AS
SELECT 
    processing_status,
    COUNT(*) as count
FROM raw_happiness_events
GROUP BY processing_status;

-- 6. Features Drift Over Time (For Line Chart)
CREATE OR REPLACE VIEW vw_features_drift AS
SELECT 
    d.year,
    AVG(r.gdp) as avg_gdp,
    AVG(r.family) as avg_family,
    AVG(r.health) as avg_health,
    AVG(r.freedom) as avg_freedom
FROM dim_raw_event r
JOIN fact_predictions f ON r.raw_event_id = f.raw_event_id
JOIN dim_date d ON f.date_id = d.date_id
GROUP BY d.year
ORDER BY d.year;
