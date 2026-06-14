-- CHECK 3 — NULL primary identifiers in src_network_sessions
/*
Problem : session_id or customer_id is NULL.
Risk    : A NULL session_id blocks deduplication.
          A NULL customer_id means data consumption
          cannot be attributed to any customer,
          causing usage metrics (total_data_used_mb,
          session counts) to be understated.
*/


SELECT
    session_id,
    customer_id,
    start_time,
    end_time,
    data_used_mb,
    'NULL session_id or customer_id' AS issue
FROM src_network_sessions
WHERE session_id   IS NULL
   OR customer_id  IS NULL;


-- CHECK 4 — Duplicate session_ids in src_network_sessions

/*
Problem : The same session_id appears more than once          
             (caused by network-logger retry events).
    Risk    : Duplicate sessions inflate data-usage totals
             and session counts, making customers appear
             more active than they are and masking real
             churn signals.*/
SELECT
    session_id,
    COUNT(*) AS occurrences
FROM src_network_sessions
WHERE session_id IS NOT NULL
GROUP BY session_id
HAVING COUNT(*) > 1;

-- Quarantine: NULL identifiers in sessions
INSERT INTO quarantine (
    row_data, 
    source_table, 
    issue_type
)
SELECT
    TO_JSONB(s),
    'src_network_sessions',
    'NULL session_id or customer_id'
FROM src_network_sessions s
WHERE (s.session_id  IS NULL
   OR s.customer_id IS NULL)
  AND NOT EXISTS (
        SELECT 1 FROM quarantine q
        WHERE q.source_table = 'src_network_sessions'
          AND q.row_data = TO_JSONB(s)
          AND q.issue_type = 'NULL session_id or customer_id'
      );

-- Quarantine: duplicate session_ids in sessions
INSERT INTO quarantine (
    row_data, 
    source_table, 
    issue_type
)
SELECT
    TO_JSONB(s),
    'src_network_sessions',
    'Duplicate session_id'
FROM src_network_sessions s
WHERE s.session_id IN (
    SELECT session_id
    FROM   src_network_sessions
    WHERE  session_id IS NOT NULL
    GROUP  BY session_id
    HAVING COUNT(*) > 1
)
AND NOT EXISTS (
    SELECT 1 FROM quarantine q
    WHERE  q.source_table = 'src_network_sessions'
      AND  q.row_data = TO_JSONB(s)
      AND q.issue_type = 'Duplicate session_id'
);
