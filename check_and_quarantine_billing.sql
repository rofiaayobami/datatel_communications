-- CHECK 1 — NULL primary identifiers in src_billing_transactions
/*
Problem : transaction_id or customer_id is NULL.
   Risk    : A NULL transaction_id makes it impossible          
             to deduplicate records. A NULL customer_id
             means revenue cannot be attributed to any   
             customer, resulting in untraceable financial 
             transactions and unbillable data usage events. */
SELECT
    transaction_id,
    customer_id,
    amount,
    transaction_date,
    'NULL transaction_id or customer_id' AS issue
FROM src_billing_transactions
WHERE transaction_id IS NULL
   OR customer_id    IS NULL;

-- CHECK 2 — Duplicate transaction_ids in src_billing_transactions
/*
Problem : The same transaction_id appears more than
             once (caused by billing-system retry events).
   Risk    : If duplicates reach the staging layer,
             revenue aggregations (SUM of amount) will
             be inflated for every affected customer,
             producing incorrect ARPU and total-revenue
             figures that are hard to detect after the fact.*/
             

SELECT
    transaction_id,
    COUNT(*) AS occurrences
FROM src_billing_transactions
WHERE transaction_id IS NOT NULL
GROUP BY transaction_id
HAVING COUNT(*) > 1;

-- Quarantine: NULL identifiers in billing
INSERT INTO quarantine (
    row_data, 
    source_table, 
    issue_type
)
SELECT
    TO_JSONB(b),
    'src_billing_transactions',
    'NULL transaction_id or customer_id'
FROM src_billing_transactions b
WHERE (b.transaction_id IS NULL
   OR b.customer_id    IS NULL)
  AND NOT EXISTS (
        SELECT 1 FROM quarantine q
        WHERE q.source_table = 'src_billing_transactions'
          AND q.row_data = TO_JSONB(b)
      );


-- Quarantine: duplicate transaction_ids in billing

INSERT INTO quarantine (
    row_data,
    source_table,
    issue_type
)
SELECT
    TO_JSONB(b),
    'src_billing_transactions',
    'Duplicate transaction_id'
FROM src_billing_transactions b
WHERE b.transaction_id IN (
    SELECT transaction_id
    FROM src_billing_transactions
    WHERE transaction_id IS NOT NULL
    GROUP BY transaction_id
    HAVING COUNT(*) > 1
)
AND NOT EXISTS (
    SELECT 1
    FROM quarantine q
    WHERE q.source_table = 'src_billing_transactions'
      AND q.row_data = TO_JSONB(b)
      AND q.issue_type = 'Duplicate transaction_id'
);
