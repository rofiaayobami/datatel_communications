-- Stage 4 — Data Warehouse Table: dw_user_analytics

/*Join Strategy — written justification:
 The anchor of the join is stg_customers. Every other
 table is LEFT JOINed to it. This guarantees that all
 customers appear in the output — even those who have
 no billing history and no sessions (e.g. newly
 registered customers who have not yet transacted).

 I deliberately do NOT start from stg_billing or
 stg_sessions as the anchor, because that would silently
 exclude customers who have no activity, making the
 warehouse incomplete.

 COALESCE(..., 0) is applied to every nullable metric
 column so analysts never encounter NULLs when writing
 aggregation queries — NULL arithmetic (e.g. SUM that
 includes NULL) is a common silent-error source.

 avg_data_per_session_mb is derived inline using the
 same NULLIF / COALESCE division-safety pattern used in
 agg_arpu, guarding against customers with 0 sessions.*/

/*
Purpose: Consolidate local PostgreSQL staging files and metric views 
         into a centralized local analytics warehouse view layout.
*/

-- SAFETY GATE: Ensure downstream transformation tables exist before running joins
CREATE TABLE IF NOT EXISTS agg_user_revenue (customer_id VARCHAR(50) PRIMARY KEY, 
                            total_revenue NUMERIC(12,2), 
                            total_transactions BIGINT);

CREATE TABLE IF NOT EXISTS agg_user_usage (customer_id VARCHAR(50) PRIMARY KEY, 
                            total_data_used_mb NUMERIC(12,2), 
                            avg_session_duration_sec NUMERIC(12,2), 
                            total_sessions BIGINT);

CREATE TABLE IF NOT EXISTS agg_arpu (customer_id VARCHAR(50) PRIMARY KEY, 
                            arpu NUMERIC(12,2), 
                            active_months INT);

CREATE TABLE IF NOT EXISTS agg_session_distribution (customer_id VARCHAR(50) PRIMARY KEY, 
                            short_sessions BIGINT, 
                            medium_sessions BIGINT, 
                            long_sessions BIGINT);


-- CREATE TABLE dw_user_analytics
CREATE TABLE IF NOT EXISTS dw_user_analytics (
    customer_id               VARCHAR(50) PRIMARY KEY,
    customer_name             VARCHAR(100),
    email                     VARCHAR(100),
    country                   VARCHAR(50),
    customer_since            TIMESTAMP,
    total_revenue             NUMERIC(12,2),
    total_transactions        BIGINT,
    total_data_used_mb        NUMERIC(12,2),
    avg_session_duration_sec  NUMERIC(12,2),
    total_sessions            BIGINT,
    arpu                      NUMERIC(12,2),
    short_sessions            BIGINT,
    medium_sessions           BIGINT,
    long_sessions             BIGINT,
    avg_data_per_session_mb   NUMERIC(12,2),
    last_updated              TIMESTAMP DEFAULT NOW()
);

-- Executing local UPSERT pattern to clean, update and load data safely
INSERT INTO dw_user_analytics (
    customer_id, customer_name, email, country, customer_since,
    total_revenue, total_transactions, total_data_used_mb,
    avg_session_duration_sec, total_sessions, arpu,
    short_sessions, medium_sessions, long_sessions,
    avg_data_per_session_mb, last_updated
)
SELECT
    c.customer_id,
    c.customer_name,
    c.email,
    c.country,
    -- Adjusting text profile formatting back to valid timestamps
    CAST(c.created_at AS TIMESTAMP) AS customer_since,
    COALESCE(r.total_revenue, 0) AS total_revenue,
    COALESCE(r.total_transactions, 0) AS total_transactions,
    COALESCE(u.total_data_used_mb, 0) AS total_data_used_mb,
    COALESCE(u.avg_session_duration_sec, 0) AS avg_session_duration_sec,
    COALESCE(u.total_sessions, 0) AS total_sessions,
    COALESCE(a.arpu, 0) AS arpu,
    COALESCE(sd.short_sessions, 0) AS short_sessions,
    COALESCE(sd.medium_sessions, 0) AS medium_sessions,
    COALESCE(sd.long_sessions, 0) AS long_sessions,
    COALESCE(ROUND(u.total_data_used_mb / NULLIF(u.total_sessions, 0), 2), 0) AS avg_data_per_session_mb,
    NOW() AS last_updated
FROM stg_customers c
LEFT JOIN agg_user_revenue r ON c.customer_id = r.customer_id
LEFT JOIN agg_user_usage u ON c.customer_id = u.customer_id
LEFT JOIN agg_arpu a ON c.customer_id = a.customer_id
LEFT JOIN agg_session_distribution sd ON c.customer_id = sd.customer_id

ON CONFLICT (customer_id) DO UPDATE SET
    customer_name            = EXCLUDED.customer_name,
    email                    = EXCLUDED.email,
    country                  = EXCLUDED.country,
    customer_since           = EXCLUDED.customer_since,
    total_revenue            = EXCLUDED.total_revenue,
    total_transactions       = EXCLUDED.total_transactions,
    total_data_used_mb       = EXCLUDED.total_data_used_mb,
    avg_session_duration_sec = EXCLUDED.avg_session_duration_sec,
    total_sessions           = EXCLUDED.total_sessions,
    arpu                     = EXCLUDED.arpu,
    short_sessions           = EXCLUDED.short_sessions,
    medium_sessions          = EXCLUDED.medium_sessions,
    long_sessions            = EXCLUDED.long_sessions,
    avg_data_per_session_mb  = EXCLUDED.avg_data_per_session_mb,
    last_updated             = EXCLUDED.last_updated;
