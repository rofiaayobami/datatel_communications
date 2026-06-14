--Stage 1 — Data Quality Checks

-- QUARANTINE TABLE 

CREATE TABLE IF NOT EXISTS quarantine (
    quarantine_id BIGSERIAL PRIMARY KEY,
    row_data      JSONB        NOT NULL,
    source_table  VARCHAR(50)  NOT NULL,
    -- Added issue_type to improve auditability and debugging
    issue_type    VARCHAR(100) NOT NULL, 
    detected_at   TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);