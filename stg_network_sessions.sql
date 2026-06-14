-- stg_sessions
/*Changes applied:
  • Deduplicate by session_id (keep most-recent row)
  • Cast start_time / end_time text → TIMESTAMPTZ
  • Replace NULL data_used_mb with 0
  • Add session_duration_sec:
      – EXTRACT(EPOCH FROM (end_time - start_time))
        when end_time > start_time
      – 0 otherwise (clock-sync errors, equal times)
  • Exclude rows where session_id IS NULL*/

CREATE TABLE IF NOT EXISTS stg_sessions (
    session_id            VARCHAR(20)   PRIMARY KEY,
    customer_id           VARCHAR(20),
    start_time            TIMESTAMPTZ,
    end_time              TIMESTAMPTZ,
    data_used_mb          NUMERIC(10,2) NOT NULL DEFAULT 0,
    session_date          DATE,
    session_duration_sec  INTEGER       NOT NULL DEFAULT 0
);

INSERT INTO stg_sessions (
    session_id, customer_id,
    start_time, end_time,
    data_used_mb, session_date,
    session_duration_sec
)
SELECT DISTINCT ON (session_id)
    session_id,
    customer_id,
    CAST(start_time AS TIMESTAMPTZ)                             AS start_time,
    CAST(end_time   AS TIMESTAMPTZ)                             AS end_time,
    COALESCE(data_used_mb, 0)                                   AS data_used_mb,
    CAST(start_time AS DATE)                                  AS session_date,
    CASE
        WHEN CAST(end_time AS TIMESTAMPTZ)
           > CAST(start_time AS TIMESTAMPTZ)
        THEN EXTRACT(
                EPOCH FROM (
                    CAST(end_time   AS TIMESTAMPTZ)
                  - CAST(start_time AS TIMESTAMPTZ)
                )
             )::INTEGER
        ELSE 0
    END                                              
       AS session_duration_sec
FROM src_network_sessions
WHERE session_id IS NOT NULL
  AND customer_id IS NOT NULL
ORDER BY session_id,
        CAST(start_time AS TIMESTAMPTZ) DESC NULLS LAST -- keep most-recent duplicate (NULLs last in case of missing start times)
ON CONFLICT (session_id) DO UPDATE SET
    customer_id           = EXCLUDED.customer_id,
    start_time            = EXCLUDED.start_time,
    end_time              = EXCLUDED.end_time,
    data_used_mb          = EXCLUDED.data_used_mb,
    session_date          = EXCLUDED.session_date,
    session_duration_sec  = EXCLUDED.session_duration_sec;

