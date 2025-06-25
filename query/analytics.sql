-- =====================================================
-- Microsoft Fabric Data Warehouse SQL Queries
-- UberEats Analytics - Medallion Architecture
-- =====================================================

-- =====================================================
-- DATA EXPLORATION QUERIES
-- =====================================================

-- Basic Data Overview - Check row counts across all tables
SELECT 'Bronze Drivers' as table_name, COUNT(*) as row_count FROM df2_bronze_postgres_drivers
UNION ALL
SELECT 'Bronze Orders', COUNT(*) FROM nb_bronze_kafka_orders
UNION ALL
SELECT 'Bronze Status', COUNT(*) FROM pl_bronze_kafka_status
UNION ALL
SELECT 'Silver Drivers', COUNT(*) FROM silver_drivers
UNION ALL
SELECT 'Silver Orders', COUNT(*) FROM silver_orders
UNION ALL
SELECT 'Silver Status', COUNT(*) FROM silver_status;

-- Schema Exploration - Explore table schemas and structure
SELECT
    TABLE_NAME,
    COLUMN_NAME,
    DATA_TYPE,
    IS_NULLABLE
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_SCHEMA = 'dbo'
ORDER BY TABLE_NAME, ORDINAL_POSITION;

-- =====================================================
-- BUSINESS ANALYTICS QUERIES
-- =====================================================

-- Driver Performance Analysis - Top drivers by vehicle type and experience level
SELECT
    vehicle_type,
    vehicle_category,
    experience_level,
    COUNT(*) as driver_count,
    AVG(quality_score) as avg_quality_score
FROM silver_drivers
WHERE is_valid = 1
GROUP BY vehicle_type, vehicle_category, experience_level
ORDER BY driver_count DESC;

-- Order Patterns Analysis - Order patterns by time period and day of week
SELECT
    order_day_of_week,
    time_period,
    delivery_urgency,
    COUNT(*) as order_count,
    AVG(total_amount) as avg_order_value,
    SUM(total_amount) as total_revenue
FROM silver_orders
GROUP BY order_day_of_week, time_period, delivery_urgency
ORDER BY order_count DESC;

-- Order Status Tracking - Order status progression analysis
SELECT
    status_category,
    status_name,
    status_priority,
    COUNT(*) as status_count,
    COUNT(DISTINCT order_id) as unique_orders
FROM silver_status
GROUP BY status_category, status_name, status_priority
ORDER BY status_category, status_count DESC;

-- Vehicle Distribution Analysis - Vehicle types and age distribution
SELECT
    vehicle_category,
    vehicle_type,
    experience_level,
    COUNT(*) as vehicle_count,
    AVG(vehicle_age_years) as avg_vehicle_age,
    MIN(vehicle_age_years) as min_vehicle_age,
    MAX(vehicle_age_years) as max_vehicle_age
FROM silver_drivers
WHERE is_valid = 1
GROUP BY vehicle_category, vehicle_type, experience_level
ORDER BY vehicle_count DESC;

-- =====================================================
-- CROSS-TABLE ANALYSIS (USING JOINS)
-- =====================================================

-- Complete Order Journey - Order journey with status progression
SELECT
    o.order_id,
    o.total_amount,
    o.order_day_of_week,
    o.time_period,
    o.delivery_urgency,
    s.status_name,
    s.status_category,
    s.status_datetime,
    s.is_business_hours
FROM silver_orders o
LEFT JOIN silver_status s ON o.order_id = s.order_id
WHERE o.total_amount > 50  -- High-value orders
ORDER BY o.order_id, s.status_datetime;

-- Driver Assignment Analysis - Orders with driver details (when available)
SELECT
    o.order_id,
    o.total_amount,
    o.order_value_category,
    o.delivery_urgency,
    d.full_name as driver_name,
    d.vehicle_type,
    d.vehicle_category,
    d.city as driver_city,
    d.experience_level
FROM silver_orders o
INNER JOIN silver_drivers d ON CAST(o.driver_id AS VARCHAR) = CAST(d.driver_id AS VARCHAR)
WHERE o.is_valid = 1 AND d.is_valid = 1
ORDER BY o.total_amount DESC;

-- Order Status Summary - Count of orders by final status
SELECT
    final_status.status_category,
    final_status.status_name,
    COUNT(DISTINCT final_status.order_id) as orders_with_status,
    AVG(o.total_amount) as avg_order_value
FROM (
    SELECT
        order_id,
        status_name,
        status_category,
        ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY status_datetime DESC) as rn
    FROM silver_status
    WHERE status_datetime IS NOT NULL
) final_status
LEFT JOIN silver_orders o ON final_status.order_id = o.order_id
WHERE final_status.rn = 1  -- Latest status per order
GROUP BY final_status.status_category, final_status.status_name
ORDER BY orders_with_status DESC;

-- =====================================================
-- DATA QUALITY MONITORING
-- =====================================================

-- Quality Score Analysis - Data quality overview across all tables
SELECT
    'Drivers' as table_name,
    AVG(quality_score) as avg_quality_score,
    MIN(quality_score) as min_quality_score,
    MAX(quality_score) as max_quality_score,
    COUNT(CASE WHEN is_valid = 1 THEN 1 END) as valid_records,
    COUNT(*) as total_records,
    ROUND(CAST(COUNT(CASE WHEN is_valid = 1 THEN 1 END) AS FLOAT) * 100.0 / COUNT(*), 2) as pct_valid
FROM silver_drivers
UNION ALL
SELECT
    'Orders',
    AVG(quality_score),
    MIN(quality_score),
    MAX(quality_score),
    COUNT(CASE WHEN is_valid = 1 THEN 1 END),
    COUNT(*),
    ROUND(CAST(COUNT(CASE WHEN is_valid = 1 THEN 1 END) AS FLOAT) * 100.0 / COUNT(*), 2)
FROM silver_orders
UNION ALL
SELECT
    'Status',
    AVG(quality_score),
    MIN(quality_score),
    MAX(quality_score),
    COUNT(CASE WHEN is_valid = 1 THEN 1 END),
    COUNT(*),
    ROUND(CAST(COUNT(CASE WHEN is_valid = 1 THEN 1 END) AS FLOAT) * 100.0 / COUNT(*), 2)
FROM silver_status;

-- Data Validation Checks - Check for orphaned records and data quality issues
SELECT
    COUNT(*) as total_orders,
    COUNT(CASE WHEN driver_id IS NULL THEN 1 END) as orders_without_driver,
    COUNT(CASE WHEN is_valid = 0 THEN 1 END) as invalid_orders,
    COUNT(CASE WHEN total_amount <= 0 THEN 1 END) as orders_with_invalid_amount,
    ROUND(CAST(COUNT(CASE WHEN driver_id IS NULL THEN 1 END) AS FLOAT) * 100.0 / COUNT(*), 2) as pct_orders_without_driver,
    ROUND(CAST(COUNT(CASE WHEN is_valid = 0 THEN 1 END) AS FLOAT) * 100.0 / COUNT(*), 2) as pct_invalid_orders
FROM silver_orders;

-- Missing Data Analysis - Identify missing or null values across key columns
SELECT
    'silver_drivers' as table_name,
    'phone_number' as column_name,
    COUNT(CASE WHEN phone_number IS NULL OR phone_number = '' THEN 1 END) as null_count,
    COUNT(*) as total_count
FROM silver_drivers
UNION ALL
SELECT 'silver_drivers', 'vehicle_make',
    COUNT(CASE WHEN vehicle_make IS NULL OR vehicle_make = '' THEN 1 END), COUNT(*) FROM silver_drivers
UNION ALL
SELECT 'silver_orders', 'driver_id',
    COUNT(CASE WHEN driver_id IS NULL THEN 1 END), COUNT(*) FROM silver_orders
UNION ALL
SELECT 'silver_orders', 'total_amount',
    COUNT(CASE WHEN total_amount IS NULL THEN 1 END), COUNT(*) FROM silver_orders
ORDER BY table_name, column_name;

-- =====================================================
-- TIME-BASED ANALYSIS
-- =====================================================

-- Temporal Patterns - Order volume by hour and business patterns
SELECT
    order_hour,
    COUNT(*) as order_count,
    AVG(total_amount) as avg_order_value,
    COUNT(CASE WHEN is_weekend = 1 THEN 1 END) as weekend_orders,
    COUNT(CASE WHEN is_weekend = 0 THEN 1 END) as weekday_orders,
    SUM(total_amount) as total_revenue
FROM silver_orders
GROUP BY order_hour
ORDER BY order_hour;

-- Weekly Order Distribution - Orders by day of week with revenue analysis
SELECT
    order_day_of_week,
    COUNT(*) as order_count,
    SUM(total_amount) as total_revenue,
    AVG(total_amount) as avg_order_value,
    COUNT(CASE WHEN delivery_urgency = 'High' THEN 1 END) as high_urgency_orders,
    ROUND(CAST(COUNT(CASE WHEN delivery_urgency = 'High' THEN 1 END) AS FLOAT) * 100.0 / COUNT(*), 2) as pct_high_urgency
FROM silver_orders
GROUP BY order_day_of_week
ORDER BY
    CASE order_day_of_week
        WHEN 'Monday' THEN 1
        WHEN 'Tuesday' THEN 2
        WHEN 'Wednesday' THEN 3
        WHEN 'Thursday' THEN 4
        WHEN 'Friday' THEN 5
        WHEN 'Saturday' THEN 6
        WHEN 'Sunday' THEN 7
    END;

-- Status Processing Times - Average time between status updates
WITH status_progression AS (
    SELECT
        order_id,
        status_name,
        status_datetime,
        LAG(status_datetime) OVER (PARTITION BY order_id ORDER BY status_datetime) as prev_status_time
    FROM silver_status
    WHERE status_datetime IS NOT NULL
)
SELECT
    status_name,
    COUNT(*) as status_count,
    AVG(DATEDIFF(minute, prev_status_time, status_datetime)) as avg_minutes_from_prev_status,
    MIN(DATEDIFF(minute, prev_status_time, status_datetime)) as min_minutes_from_prev,
    MAX(DATEDIFF(minute, prev_status_time, status_datetime)) as max_minutes_from_prev
FROM status_progression
WHERE prev_status_time IS NOT NULL
GROUP BY status_name
ORDER BY avg_minutes_from_prev_status DESC;

-- Business Hours Analysis - Order activity during business vs non-business hours
SELECT
    'Business Hours (9-18)' as time_category,
    COUNT(*) as order_count,
    AVG(total_amount) as avg_order_value,
    SUM(total_amount) as total_revenue
FROM silver_orders o
INNER JOIN silver_status s ON o.order_id = s.order_id
WHERE s.is_business_hours = 1
UNION ALL
SELECT
    'Non-Business Hours',
    COUNT(*) as order_count,
    AVG(total_amount) as avg_order_value,
    SUM(total_amount) as total_revenue
FROM silver_orders o
INNER JOIN silver_status s ON o.order_id = s.order_id
WHERE s.is_business_hours = 0;

-- =====================================================
-- ADVANCED ANALYTICS
-- =====================================================

-- Revenue Analysis - Revenue trends by order size and time period
SELECT
    order_size_category,
    time_period,
    COUNT(*) as order_count,
    SUM(total_amount) as total_revenue,
    AVG(total_amount) as avg_order_value,
    MIN(total_amount) as min_order_value,
    MAX(total_amount) as max_order_value,
    ROUND(SUM(total_amount) * 100.0 / SUM(SUM(total_amount)) OVER (), 2) as pct_of_total_revenue
FROM silver_orders
WHERE is_valid = 1
GROUP BY order_size_category, time_period
ORDER BY total_revenue DESC;

-- Geographic Distribution - Driver distribution by city and vehicle type
SELECT
    country,
    city,
    vehicle_type,
    vehicle_category,
    COUNT(*) as driver_count,
    AVG(vehicle_age_years) as avg_vehicle_age,
    COUNT(CASE WHEN driver_profile_completeness = 'Complete' THEN 1 END) as complete_profiles
FROM silver_drivers
WHERE is_valid = 1
GROUP BY country, city, vehicle_type, vehicle_category
HAVING COUNT(*) > 1  -- Only cities with multiple drivers
ORDER BY driver_count DESC;

-- Order Value Distribution - Analyze order value patterns and categories
SELECT
    order_value_category,
    order_size_category,
    COUNT(*) as order_count,
    AVG(total_amount) as avg_amount,
    MIN(total_amount) as min_amount,
    MAX(total_amount) as max_amount,
    STDEV(total_amount) as amount_std_dev
FROM silver_orders
WHERE is_valid = 1
GROUP BY order_value_category, order_size_category
ORDER BY order_value_category, order_size_category;

-- Driver Utilization Analysis - Driver activity and performance metrics
SELECT
    d.vehicle_category,
    d.experience_level,
    d.city,
    COUNT(DISTINCT d.driver_id) as unique_drivers,
    COUNT(o.order_id) as total_orders,
    CASE
        WHEN COUNT(DISTINCT d.driver_id) > 0
        THEN ROUND(CAST(COUNT(o.order_id) AS FLOAT) / COUNT(DISTINCT d.driver_id), 2)
        ELSE 0
    END as avg_orders_per_driver,
    AVG(o.total_amount) as avg_order_value
FROM silver_drivers d
LEFT JOIN silver_orders o ON CAST(d.driver_id AS VARCHAR) = CAST(o.driver_id AS VARCHAR) AND o.is_valid = 1
WHERE d.is_valid = 1
GROUP BY d.vehicle_category, d.experience_level, d.city
ORDER BY total_orders DESC;

-- =====================================================
-- DATA LINEAGE AND AUDIT QUERIES
-- =====================================================

-- Data Source Tracking - Track data sources and ingestion methods
SELECT
    data_source,
    COUNT(*) as record_count,
    MIN(silver_load_timestamp) as first_load,
    MAX(silver_load_timestamp) as last_load,
    DATEDIFF(minute, MIN(silver_load_timestamp), MAX(silver_load_timestamp)) as load_duration_minutes
FROM (
    SELECT data_source, silver_load_timestamp FROM silver_drivers
    UNION ALL
    SELECT data_source, silver_load_timestamp FROM silver_orders
    UNION ALL
    SELECT data_source, silver_load_timestamp FROM silver_status
) combined
GROUP BY data_source
ORDER BY record_count DESC;

-- Record Hash Analysis - Check for duplicate records using hash values
SELECT
    'silver_drivers' as table_name,
    COUNT(*) as total_records,
    COUNT(DISTINCT record_hash) as unique_hashes,
    COUNT(*) - COUNT(DISTINCT record_hash) as potential_duplicates
FROM silver_drivers
UNION ALL
SELECT
    'silver_orders',
    COUNT(*),
    COUNT(DISTINCT record_hash),
    COUNT(*) - COUNT(DISTINCT record_hash)
FROM silver_orders
UNION ALL
SELECT
    'silver_status',
    COUNT(*),
    COUNT(DISTINCT record_hash),
    COUNT(*) - COUNT(DISTINCT record_hash)
FROM silver_status;

-- Data Freshness Analysis - Check when data was last updated
SELECT
    'silver_drivers' as table_name,
    MAX(updated_at) as last_updated,
    MIN(updated_at) as first_updated,
    COUNT(*) as total_records
FROM silver_drivers
UNION ALL
SELECT
    'silver_orders',
    MAX(updated_at),
    MIN(updated_at),
    COUNT(*)
FROM silver_orders
UNION ALL
SELECT
    'silver_status',
    MAX(updated_at),
    MIN(updated_at),
    COUNT(*)
FROM silver_status
ORDER BY table_name;

-- =====================================================
-- BUSINESS INTELLIGENCE QUERIES
-- =====================================================

-- Top Performing Cities - Cities with highest order values and driver activity
WITH city_metrics AS (
    SELECT
        d.city,
        d.country,
        COUNT(DISTINCT d.driver_id) as driver_count,
        COUNT(o.order_id) as order_count,
        SUM(o.total_amount) as total_revenue,
        AVG(o.total_amount) as avg_order_value
    FROM silver_drivers d
    LEFT JOIN silver_orders o ON CAST(d.driver_id AS VARCHAR) = CAST(o.driver_id AS VARCHAR)
    WHERE d.is_valid = 1 AND (o.is_valid = 1 OR o.order_id IS NULL)
    GROUP BY d.city, d.country
)
SELECT
    city,
    country,
    driver_count,
    order_count,
    total_revenue,
    avg_order_value,
    CASE
        WHEN driver_count > 0 THEN ROUND(order_count * 1.0 / driver_count, 2)
        ELSE 0
    END as orders_per_driver
FROM city_metrics
WHERE driver_count > 0
ORDER BY total_revenue DESC, order_count DESC;

-- Order Completion Rate - Calculate completion rates by status category
WITH order_statuses AS (
    SELECT
        s.order_id,
        MAX(CASE WHEN s.status_category = 'Final' THEN 1 ELSE 0 END) as is_completed,
        COUNT(DISTINCT s.status_category) as status_stages,
        o.total_amount,
        o.order_value_category
    FROM silver_status s
    INNER JOIN silver_orders o ON s.order_id = o.order_id
    WHERE s.is_valid = 1 AND o.is_valid = 1
    GROUP BY s.order_id, o.total_amount, o.order_value_category
)
SELECT
    order_value_category,
    COUNT(*) as total_orders,
    SUM(is_completed) as completed_orders,
    ROUND(SUM(is_completed) * 100.0 / COUNT(*), 2) as completion_rate_pct,
    AVG(status_stages) as avg_status_stages,
    AVG(total_amount) as avg_order_value
FROM order_statuses
GROUP BY order_value_category
ORDER BY completion_rate_pct DESC;

-- =====================================================
-- PERFORMANCE AND MAINTENANCE QUERIES
-- =====================================================

-- Table Size Analysis - Analyze table sizes and row counts
SELECT
    TABLE_NAME,
    TABLE_ROWS as estimated_rows,
    ROUND((DATA_LENGTH + INDEX_LENGTH) / 1024 / 1024, 2) as size_mb
FROM INFORMATION_SCHEMA.TABLES
WHERE TABLE_SCHEMA = 'dbo'
    AND TABLE_TYPE = 'BASE TABLE'
ORDER BY size_mb DESC;

-- Data Distribution Analysis - Check data distribution across tables
SELECT
    'Orders by Month' as metric,
    order_month,
    COUNT(*) as count
FROM silver_orders
GROUP BY order_month
ORDER BY order_month
UNION ALL
SELECT
    'Orders by Year',
    order_year,
    COUNT(*)
FROM silver_orders
GROUP BY order_year
ORDER BY order_year;
