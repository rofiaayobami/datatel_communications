--session_buckets
--Classify every session as short / medium / long 

DROP TABLE IF EXISTS session_buckets;
CREATE TABLE session_buckets (
    session_id    VARCHAR(20) PRIMARY KEY,
    customer_id   VARCHAR(20),
    duration_sec  NUMERIC(10,2),
    bucket        VARCHAR(10)
);

INSERT INTO session_buckets (session_id, customer_id, duration_sec, bucket)
SELECT
    session_id,
    customer_id,
    session_duration_sec,
    CASE
        WHEN session_duration_sec < 60  THEN 'short'
        WHEN session_duration_sec < 300 THEN 'medium'
        ELSE                                 'long'
    END AS bucket
FROM stg_sessions
WHERE customer_id IS NOT NULL;

