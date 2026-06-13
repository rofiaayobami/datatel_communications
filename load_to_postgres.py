import pandas as pd
import psycopg2
from dotenv import load_dotenv 
from psycopg2.extras import execute_values
import os
from pathlib import Path  

# Load environment variables
load_dotenv()

DB_CONFIG = {
    "host": os.getenv("POSTGRES_HOST"),
    "port": os.getenv("POSTGRES_PORT"),
    "dbname": os.getenv("POSTGRES_DB"),
    "user": os.getenv("POSTGRES_USER"),
    "password": os.getenv("POSTGRES_PASSWORD"),
}

CSV_DIR = os.getenv("CSV_DIR")

print("Connecting to PostgreSQL...")
conn = psycopg2.connect(**DB_CONFIG)
conn.autocommit = False
cur = conn.cursor()
print("Connected")

# HELPER

def load_csv_to_table(csv_file, table_name, create_ddl, columns):
    path = os.path.join(CSV_DIR, csv_file)
    print(f"Loading {csv_file} into {table_name}..")

    print(f"   Reading CSV...")
    df = pd.read_csv(path)

    # Replace NaN with None so psycopg2 writes NULL

    df = df.where(pd.notnull(df), None)
    rows = [tuple(row) for row in df[columns].itertuples(index=False)]
    print(f"   {len(rows):,} rows read")

    print(f"   Creating table...")
    cur.execute(f"DROP TABLE IF EXISTS {table_name};")
    cur.execute(create_ddl)

    print(f"   Inserting rows (batch mode)...")
    col_str = ", ".join(columns)
    placeholders = ", ".join(["%s"] * len(columns))
    execute_values(
        cur,
        f"INSERT INTO {table_name} ({col_str}) VALUES %s",
        rows,
        page_size=10_000,
    )
    conn.commit()
    print(f"{table_name} loaded — {len(rows):,} rows")


# src_customers

load_csv_to_table(
    csv_file   = "src_customers.csv",
    table_name = "src_customers",
    create_ddl = """
        CREATE TABLE src_customers (
            customer_id   VARCHAR(20),
            name          VARCHAR(100),
            email         VARCHAR(100),
            country       VARCHAR(50),
            created_at    VARCHAR(30),
            ingested_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """,
    columns = ["customer_id", "name", "email", "country", "created_at"],
)

# src_billing_transactions

load_csv_to_table(
    csv_file   = "src_billing_transactions.csv",
    table_name = "src_billing_transactions",
    create_ddl = """
        CREATE TABLE src_billing_transactions (
            transaction_id   VARCHAR(20),
            customer_id      VARCHAR(20),
            amount           NUMERIC(12,2),
            transaction_date VARCHAR(30),
            ingested_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """,
    columns = ["transaction_id", "customer_id", "amount", "transaction_date"],
)

# src_network_sessions

load_csv_to_table(
    csv_file   = "src_network_sessions.csv",
    table_name = "src_network_sessions",
    create_ddl = """
        CREATE TABLE src_network_sessions (
            session_id    VARCHAR(20),
            customer_id   VARCHAR(20),
            start_time    VARCHAR(30),
            end_time      VARCHAR(30),
            data_used_mb  NUMERIC(10,2),
            ingested_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """,
    columns = ["session_id", "customer_id", "start_time", "end_time", "data_used_mb"],
)

# VERIFY

print(" Row counts:")
for table in ["src_customers", "src_billing_transactions", "src_network_sessions"]:
    cur.execute(f"SELECT COUNT(*) FROM {table};")
    count = cur.fetchone()[0]
    print(f"   {table}: {count:,}")

cur.close()
conn.close()
print(" All done!!")