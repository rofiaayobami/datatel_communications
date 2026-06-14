-- Stage 5 — Analytical Queries
-- Purpose : Answer real business questions directly from
--           dw_user_analytics and the transformation tables.
--           These are ad-hoc queries — results are not stored.


-- Q1. Top 10 most valuable customers by total revenue

SELECT
    customer_id,
    customer_name,
    country,
    total_revenue,
    total_transactions,
    arpu,
    total_sessions
FROM `dw_user_analytics`
ORDER BY total_revenue DESC
LIMIT 10;

-- Q2. Churn risk — customers with low activity and low spend
-- Fixed: Uses BigQuery DATE_DIFF and TIMESTAMP_SUB syntax


SELECT
    customer_id,
    customer_name,
    email,
    total_sessions,
    total_revenue,
    customer_since,
    DATE_DIFF(CURRENT_DATE(), DATE(customer_since), DAY) AS days_since_registration
FROM `dw_user_analytics`
WHERE total_sessions < 5
  AND total_revenue  < 1000
  AND customer_since < TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 DAY)
ORDER BY total_revenue ASC, total_sessions ASC;


-- Q3. Revenue vs data usage mismatch

SELECT
    customer_id,
    customer_name,
    total_revenue,
    total_data_used_mb,
    avg_data_per_session_mb,
    CASE
        WHEN total_data_used_mb > 500 AND total_revenue < 3000
            THEN 'High usage / low spend'
        WHEN total_data_used_mb < 100 AND total_revenue > 5000
            THEN 'Low usage / high spend'
        ELSE 'Normal'
    END AS revenue_usage_flag
FROM `dw_user_analytics`
WHERE (total_data_used_mb > 500 AND total_revenue < 3000)
   OR (total_data_used_mb < 100 AND total_revenue > 5000)
ORDER BY revenue_usage_flag, total_revenue DESC;


-- Q4. Monthly revenue trend (all customers combined)
-- Note: Run this query on your Postgres DB or wherever agg_monthly_revenue sits

SELECT
    revenue_month,
    SUM(monthly_revenue)        AS total_monthly_revenue,
    COUNT(DISTINCT customer_id) AS active_customers
FROM agg_monthly_revenue
GROUP BY revenue_month
ORDER BY revenue_month;


-- Q5. Session behaviour distribution summary
-- Fixed: Explicit FLOAT64 type promotion for clean cloud division

SELECT
    customer_id,
    customer_name,
    short_sessions,
    medium_sessions,
    long_sessions,
    total_sessions,
    ROUND(100.0 * CAST(long_sessions AS FLOAT64)  / NULLIF(total_sessions, 0), 1) AS pct_long,
    ROUND(100.0 * CAST(short_sessions AS FLOAT64) / NULLIF(total_sessions, 0), 1) AS pct_short
FROM `dw_user_analytics`
WHERE total_sessions > 0
ORDER BY pct_long DESC;

