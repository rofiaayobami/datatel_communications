-- agg_monthly_revenue
--Per customer per calendar month: total revenue

DROP TABLE IF EXISTS agg_monthly_revenue;
CREATE TABLE agg_monthly_revenue (
    customer_id     VARCHAR(20),
    revenue_month   DATE,
    monthly_revenue NUMERIC(14,2),
    PRIMARY KEY (customer_id, revenue_month)
);

INSERT INTO agg_monthly_revenue (customer_id, revenue_month, monthly_revenue)
SELECT
    customer_id,
    DATE_TRUNC('month', transaction_date)::DATE  AS revenue_month,
    COALESCE(SUM(amount), 0)                     AS monthly_revenue
FROM stg_billing
WHERE customer_id IS NOT NULL
GROUP BY customer_id, DATE_TRUNC('month', transaction_date);

