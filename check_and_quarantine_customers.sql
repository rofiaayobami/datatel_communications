-- CHECK 5 — Duplicate customer_ids in src_customers
/*
Problem : The same customer_id appears more than once.
Risk     : The customer table serves as the single 
           source of truth for dimensional identity.
*/

SELECT
    customer_id,
    COUNT(*) AS occurrences
FROM src_customers
WHERE customer_id IS NOT NULL
GROUP BY customer_id
HAVING COUNT(*) > 1;

-- Quarantine: NULL customer_id in customers
INSERT INTO quarantine (
    row_data, 
    source_table, 
    issue_type
)
SELECT
    TO_JSONB(c),
    'src_customers',
    'NULL customer_id'
FROM src_customers c
WHERE c.customer_id IS NULL
  AND NOT EXISTS (
        SELECT 1 FROM quarantine q
        WHERE q.source_table = 'src_customers'
          AND q.row_data = TO_JSONB(c)
            AND q.issue_type = 'NULL customer_id'
      );


-- Quarantine: duplicate customer_id in customers

INSERT INTO quarantine (
    row_data,
    source_table,
    issue_type
)
SELECT
    TO_JSONB(c),
    'src_customers',
    'Duplicate customer_id'
FROM src_customers c
WHERE c.customer_id IN (
    SELECT customer_id
    FROM src_customers
    WHERE customer_id IS NOT NULL
    GROUP BY customer_id
    HAVING COUNT(*) > 1
)
AND NOT EXISTS (
    SELECT 1
    FROM quarantine q
    WHERE q.source_table = 'src_customers'
      AND q.issue_type  = 'Duplicate customer_id'
      AND q.row_data    = TO_JSONB(c)
);

