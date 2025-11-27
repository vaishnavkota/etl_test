CREATE DATABASE IF NOT EXISTS housing_db;
USE housing_db;

CREATE TABLE IF NOT EXISTS housing_age_metrics (
    id INT AUTO_INCREMENT PRIMARY KEY,
    housing_median_age FLOAT,
    avg_house_value DECIMAL(15, 2),
    total_population INT,
    total_rooms_in_age_group INT,
    district_count INT,
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);