--stg_billing
/*Changes applied:
  • Deduplicate by transaction_id (keep most-recent row)
  • Replace NULL amount with 0
  • Cast transaction_date text to TIMESTAMPTZ
  • Exclude rows where transaction_id IS NULL

Deduplication approach — written justification:

  i used DISTINCT ON (transaction_id) ordered by
  transaction_date DESC so that the most recently
  recorded version of a transaction is kept.

  i did not DELETE duplicates from the source because
  The source table is raw/operational and we must never
  modify it. Quarantine captures the problem for ops.

  i did not keep the first occurrence because
  Retry events in billing systems usually carry
  corrected data (e.g., a corrected amount or a
  successful status flag). Keeping the latest record
  is therefore more likely to reflect the true
  outcome of the transaction.

  i could have deduplicated in a CTE and then INSERT but
  DISTINCT ON is set-based and runs in a single pass;
  a self-join CTE (ROW_NUMBER approach) also works but
  produces the same result with more I/O. Both are
  correct; DISTINCT ON is idiomatic PostgreSQL.*/


CREATE TABLE IF NOT EXISTS stg_billing (
    transaction_id    VARCHAR(20)   PRIMARY KEY,
    customer_id       VARCHAR(20),
    amount            NUMERIC(12,2) NOT NULL DEFAULT 0,
    transaction_date  TIMESTAMPTZ
);

INSERT INTO stg_billing (transaction_id, customer_id, amount, transaction_date)
SELECT DISTINCT ON (transaction_id)
    transaction_id,
    customer_id,
    COALESCE(amount, 0)                             AS amount,
    CAST(transaction_date AS TIMESTAMPTZ)           AS transaction_date
FROM src_billing_transactions
WHERE transaction_id IS NOT NULL
  AND customer_id IS NOT NULL
ORDER BY transaction_id,
         CAST(transaction_date AS TIMESTAMPTZ) DESC NULLS LAST -- keep most-recent duplicate (NULLs last in case of missing dates)
ON CONFLICT (transaction_id)DO UPDATE SET
    customer_id      = EXCLUDED.customer_id,
    amount           = EXCLUDED.amount,
    transaction_date = EXCLUDED.transaction_date;
