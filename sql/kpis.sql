-- 1. Average prediction error
CREATE OR REPLACE VIEW vw_avg_prediction_error AS
SELECT 
    AVG(prediction_error) as avg_error
FROM fact_predictions;

-- 2. Predictions by country
CREATE OR REPLACE VIEW vw_predictions_by_country AS
SELECT 
    c.country_name,
    COUNT(f.prediction_id) as total_predictions,
    AVG(f.actual_score) as avg_actual_score,
    AVG(f.predicted_score) as avg_predicted_score,
    AVG(f.prediction_error) as avg_error
FROM fact_predictions f
JOIN dim_country c ON f.country_id = c.country_id
GROUP BY c.country_name
ORDER BY total_predictions DESC;

-- 3. Predicted vs actual score (Detailed)
CREATE OR REPLACE VIEW vw_predicted_vs_actual AS
SELECT 
    c.country_name,
    d.year,
    f.actual_score,
    f.predicted_score,
    f.prediction_error
FROM fact_predictions f
JOIN dim_country c ON f.country_id = c.country_id
JOIN dim_date d ON f.date_id = d.date_id;

-- 4. Prediction trends over time
CREATE OR REPLACE VIEW vw_prediction_trends AS
SELECT 
    d.year,
    AVG(f.actual_score) as avg_actual_score,
    AVG(f.predicted_score) as avg_predicted_score,
    AVG(f.prediction_error) as avg_error
FROM fact_predictions f
JOIN dim_date d ON f.date_id = d.date_id
GROUP BY d.year
ORDER BY d.year ASC;
