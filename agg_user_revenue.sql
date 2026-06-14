-- agg_user_revenue
--Per customer: total revenue and transaction count

DROP TABLE IF EXISTS agg_user_revenue;
CREATE TABLE agg_user_revenue (
    customer_id         VARCHAR(20) PRIMARY KEY,
    total_revenue       NUMERIC(14,2),
    total_transactions  INTEGER
);

INSERT INTO agg_user_revenue (customer_id, total_revenue, total_transactions)
SELECT
    customer_id,
    COALESCE(SUM(amount), 0) AS total_revenue,
    COUNT(*)                 AS total_transactions
FROM stg_billing
WHERE customer_id IS NOT NULL
GROUP BY customer_id;

