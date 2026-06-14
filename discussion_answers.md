# DataTel Pipeline — Discussion Questions
## Written Answers for Submission

---

### Q1. The staging layer must be incremental. Walk through your chosen strategy: what defines the boundary between "already loaded" and "new"? What happens to a record that arrives two days late?

**Strategy chosen: date-window filtering on `transaction_date`**

The boundary between "already loaded" and "new" is defined by the `transaction_date` column in `src_billing_transactions`. Each daily run processes only records where `transaction_date` falls within `[t_start, t_end)` — typically yesterday midnight to today midnight. This window is injected into the SQL at runtime by Airflow as `{{ params.t_start }}` and `{{ params.t_end }}`.

Once a record is loaded into `stg_billing`, its `transaction_id` acts as a deduplication key. The `ON CONFLICT (transaction_id) DO NOTHING` clause ensures that even if the same window is reprocessed, already-loaded records are silently skipped — making the load **idempotent**.

**Late arrivals (records that arrive two days late):**
A record with `transaction_date = 2024-05-18` that lands in the source system on `2024-05-20` will have its `transaction_date` outside the window for the `2024-05-20` run (which processes `2024-05-20` data). It will be **missed** by the regular daily run.

To recover it, an operator uses the Airflow UI to trigger the DAG with `t_start = 2024-05-18` and `t_end = 2024-05-19`. The idempotent SQL handles this safely — records already in `stg_billing` for that window are skipped, and the late arrival is inserted cleanly.

A production enhancement would be to also monitor `_ingestion_timestamp` (when the record arrived in the source system) rather than relying solely on `transaction_date`, enabling automatic late-arrival detection.

---

### Q2. Aggregation tables like `agg_user_revenue` summarise the entire history of a customer, not just today's data. How does your incremental strategy keep these tables correct without rebuilding them from scratch every day?

The aggregation tables (`agg_user_revenue`, `agg_user_usage`, `agg_arpu`, etc.) are always computed as a **full recompute from the staging tables** — not incrementally. This is safe and efficient because:

1. **Staging tables are the source of truth.** `stg_billing` and `stg_sessions` are append-only and deduplicated. Every day new rows are added, but existing rows are never removed or changed.

2. **`ON CONFLICT ... DO UPDATE` replaces the old aggregate.** When the transformation SQL runs, it recomputes the full aggregate for every affected customer and upserts the result. The old row is replaced atomically.

3. **Why not aggregate only today's new records and add them to yesterday's total?** This approach (delta aggregation) is faster but fragile — a late arrival, a corrected transaction, or a backfill run would produce wrong totals unless handled very carefully. Full recompute from the staging layer is simpler, correct by construction, and fast enough at the data volumes a mid-sized telco like DataTel generates.

If the staging tables grow to hundreds of millions of rows and full recompute becomes too slow, the pattern to adopt is **incremental aggregation with a reconciliation window**: recompute only the last N days of aggregates, plus a weekly full-rebuild job to catch any drifts.

---

### Q3. `stg_customers` has no reliable activity timestamp to use as an incremental filter. How did you handle its loading strategy, and why?

`src_customers` has a `created_at` column but it records when the customer was first registered, not when the CRM record was last changed. A customer whose name, email, or country is updated in the CRM system will not have a new `created_at` value — making it unsuitable as an incremental filter.

**Strategy chosen: full upsert on every run.**

Every daily run re-reads the entire `src_customers` table and upserts into `stg_customers` using `ON CONFLICT (customer_id) DO UPDATE`. This means:

- New customers (new `customer_id`) are inserted.
- Updated customers (existing `customer_id` with changed attributes) have their row refreshed.
- Unchanged customers are updated in place with identical data (harmless).

**Why is this acceptable?**
Customer tables in most telcos are small relative to transactions and sessions. DataTel's customer base might be millions of rows, but a full table scan and upsert completes in seconds on modern PostgreSQL. The cost of simplicity is worth it.

**A more scalable alternative** (if the customer table grows extremely large) would be to use a CDC (Change Data Capture) tool like Debezium on the CRM database. Debezium streams only changed rows into a Kafka topic, and the pipeline consumes only deltas. This was not implemented here because it requires infrastructure beyond the scope of this project, but it is the right production-scale answer.

---

### Q4. The BigQuery final table must support both new and returning customers. What write pattern did you choose and what would break if you used a simple overwrite instead?

**Pattern chosen: MERGE (upsert)**

The `MERGE` statement in `dw_user_analytics.sql` uses `customer_id` as the match key:
- `WHEN MATCHED` → update all metric columns for returning customers
- `WHEN NOT MATCHED` → insert a new row for first-time customers

**What would break with a simple overwrite (`CREATE OR REPLACE TABLE ... AS SELECT ...`)?**

1. **Historical metadata loss.** If the source systems ever have a gap (e.g. billing data for a specific day is not yet available), a full overwrite would replace the complete table with a partial snapshot — metrics for customers who only had activity in the missing period would drop to zero, then jump back up the next day. Analysts would see incorrect trends.

2. **Race conditions.** If a dashboard query is running while an overwrite is in progress, it may read a half-written table (depending on BigQuery's transaction isolation). MERGE is atomic per row.

3. **Partition loss.** `dw_user_analytics` is partitioned by `DATE(customer_since)`. A full overwrite would rewrite all partitions every day — expensive and wasteful. MERGE only touches rows that have changed.

4. **No audit trail.** With MERGE, the `last_updated` column captures exactly when each customer's record was last refreshed. A full overwrite stamps every row with today's date, losing the ability to identify which customers had genuine updates vs which were unchanged.

---

### Q5. If the billing source system sends data six hours late one night, how does your pipeline behave? What would you add to detect and alert on this scenario?

**Current behaviour:**
The DAG runs at 03:00 WAT. If billing data for the previous day arrives at 09:00 WAT (six hours late), the `stage_billing_incremental` task will process an empty window — zero new rows are found in `src_billing_transactions` for yesterday's date range. The pipeline completes successfully with no errors, but yesterday's billing data is not loaded.

This is a **silent failure** — the pipeline reports success, but the data is missing.

**What to add:**

1. **Row-count sensor.** Add an `SqlSensor` task before `stage_billing_incremental` that waits until at least N rows exist in `src_billing_transactions` for the current date window. If the sensor times out (e.g. after 4 hours), it fails the task and triggers an alert.

2. **Post-load count check.** After `stage_billing_incremental`, add a task that compares the number of rows loaded today against a rolling 7-day average. If today's count is more than 2 standard deviations below the average, send a Slack/email alert.

3. **SLA miss alerts.** Set an SLA on the DAG (`sla=timedelta(hours=2)`). If the pipeline has not completed by 05:00 WAT, Airflow sends an SLA miss notification.

4. **Data freshness check.** Query `MAX(transaction_date)` from `stg_billing` after each run. If the maximum loaded date is more than 25 hours behind `NOW()`, fire an alert to the on-call engineer.

---

### Q6. A customer appears in `src_billing_transactions` but has no record in `src_customers`. Trace exactly what happens to their data through every stage of your pipeline. Is the outcome acceptable?

**Trace through the pipeline:**

**Stage 1 (Quality checks):** The customer's transactions pass the NULL checks (their `transaction_id` and `customer_id` are not NULL). They are not flagged as duplicates. They enter the pipeline without being quarantined. *(The quality checks do not perform referential integrity checks across tables — this is a gap.)*

**Stage 2 (Staging — `stg_billing`):** Their transactions are deduplicated and loaded into `stg_billing` normally. `stg_customers` has no row for them.

**Stage 3 (Transformations):** `agg_user_revenue`, `agg_arpu`, and `agg_session_distribution` group by `customer_id` from the staging tables. Their `customer_id` will appear in `agg_user_revenue` with a valid revenue total, but since there is no matching row in `stg_customers`, they will not appear in `agg_arpu` if it joins to `stg_customers` (it does not — it reads from `stg_billing` directly, so their ARPU is computed correctly).

**Stage 4 (`dw_user_analytics`):** The final warehouse table is anchored to `stg_customers` via a `LEFT JOIN`. A customer not in `stg_customers` will **not appear in `dw_user_analytics`**. Their revenue is real but invisible.

**Is this acceptable?**
No. Revenue attributed to an unknown customer is a data integrity problem — it causes total revenue in the warehouse to be understated.

**Fix:** Add a referential integrity check in Stage 1 that detects `customer_id` values in `src_billing_transactions` that do not exist in `src_customers`. Quarantine those transactions. Additionally, create an "Unknown Customer" placeholder row in `stg_customers` (e.g. `customer_id = 'UNKNOWN'`) and remap orphaned transactions to it during staging, so the revenue is captured even if the attribution is incomplete.

---

### Q7. The churn risk rule flags customers with fewer than 5 sessions and less than ₦1,000 in revenue. A customer who registered yesterday would always be flagged. What data is already available in your pipeline to fix this, and what change would you make?

**Data already available:** `stg_customers` contains `created_at` (cast to a proper timestamp), and `dw_user_analytics` exposes this as `customer_since`.

**The fix — add a tenure filter to the churn risk query:**

```sql
SELECT
    customer_id,
    customer_name,
    email,
    total_sessions,
    total_revenue,
    customer_since
FROM dw_user_analytics
WHERE total_sessions < 5
  AND total_revenue  < 1000
  AND customer_since < (CURRENT_TIMESTAMP - INTERVAL '30 days')
ORDER BY total_revenue ASC;
```

By adding `AND customer_since < (CURRENT_TIMESTAMP - INTERVAL '30 days')`, newly registered customers (within the last 30 days) are excluded from the churn flag. They simply have not had enough time to accumulate sessions or revenue.

**A more nuanced improvement** would be to define churn thresholds as *rates* rather than raw counts:
- Instead of "fewer than 5 sessions total", use "fewer than 1 session per week since registration"
- Instead of "less than ₦1,000 total", use "less than ₦250 per week since registration"

This normalises for tenure and makes the rule fair for both new and long-standing customers. The `customer_since` and `total_sessions` columns needed to compute this are already available in `dw_user_analytics`.
