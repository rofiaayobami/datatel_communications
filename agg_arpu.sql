-- agg_arpu
 /*Per customer: average revenue per user
Definition: total_revenue / distinct active months
Division-risk handling — written justification:
  A customer with zero transactions would have
  active_months = 0, causing a division-by-zero
  error. We handle this with NULLIF(active_months, 0),
  which returns NULL instead of raising an error.
  The COALESCE(..., 0) then converts that NULL to 0,
  so every customer always has a valid ARPU value —
  either their true ARPU or 0 if they have never
  transacted. This is preferable to excluding the
  customer row, because the warehouse requires every
  customer to appear in dw_user_analytics.*/


DROP TABLE IF EXISTS agg_arpu;
CREATE TABLE agg_arpu (
    customer_id    VARCHAR(20) PRIMARY KEY,
    arpu           NUMERIC(14,2),
    active_months  INTEGER
);

INSERT INTO agg_arpu (customer_id, arpu, active_months)
SELECT
    c.customer_id,
    COALESCE(
        ROUND(
            SUM(b.amount) / NULLIF(COUNT(DISTINCT DATE_TRUNC('month', b.transaction_date)), 0), 
            2
        ),
        0
    )                                                        AS arpu,
    COUNT(DISTINCT DATE_TRUNC('month', b.transaction_date))   AS active_months
FROM stg_customers c
LEFT JOIN stg_billing b ON c.customer_id = b.customer_id
GROUP BY c.customer_id;

