/*Changes applied:
 • Deduplicate by customer_id (keep latest created_at)
 • Standardise name → Title Case (INITCAP)
 • Normalise email → lowercase
 • Fill NULL country with 'Nigeria'
 • Cast created_at text → TIMESTAMPTZ
 • Exclude rows where customer_id IS NULL*/

DROP TABLE IF EXISTS stg_customers;

CREATE TABLE stg_customers (
    customer_id    VARCHAR(20)  PRIMARY KEY,
    customer_name  VARCHAR(100),
    email          VARCHAR(100),
    country        VARCHAR(50),
    created_at     TIMESTAMPTZ,
    staged_at      TIMESTAMPTZ DEFAULT NOW() 
);


INSERT INTO stg_customers (customer_id, customer_name, email, country, created_at)
SELECT DISTINCT ON (customer_id)
    customer_id,
    INITCAP(name)                        AS customer_name,
    LOWER(email)                         AS email,
    COALESCE(country, 'Nigeria')         AS country,
    CAST(created_at AS TIMESTAMPTZ)      AS created_at
FROM src_customers
WHERE customer_id IS NOT NULL
ORDER BY customer_id,
         CAST(created_at AS TIMESTAMPTZ) DESC       
ON CONFLICT (customer_id) DO UPDATE
    SET customer_name = EXCLUDED.customer_name,
        email         = EXCLUDED.email,
        country       = EXCLUDED.country,
        created_at    = EXCLUDED.created_at;
