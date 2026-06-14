-- agg_session_distribution
--Per customer: count of short, medium, and long sessions

DROP TABLE IF EXISTS agg_session_distribution;
CREATE TABLE agg_session_distribution (
    customer_id     VARCHAR(20) PRIMARY KEY,
    short_sessions  INTEGER DEFAULT 0,
    medium_sessions INTEGER DEFAULT 0,
    long_sessions   INTEGER DEFAULT 0
);

INSERT INTO agg_session_distribution (customer_id, short_sessions, medium_sessions, long_sessions)
SELECT
    customer_id,
    COUNT(*) FILTER (WHERE bucket = 'short')   AS short_sessions,
    COUNT(*) FILTER (WHERE bucket = 'medium')  AS medium_sessions,
    COUNT(*) FILTER (WHERE bucket = 'long')    AS long_sessions
FROM session_buckets
WHERE customer_id IS NOT NULL
GROUP BY customer_id;

