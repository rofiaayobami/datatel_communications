import pandas as pd
import numpy as np
from faker import Faker
import random
from tqdm import tqdm
import time

fake = Faker()
np.random.seed(42)
random.seed(42)

# =========================
# SCALE CONFIG
# =========================
NUM_CUSTOMERS = 100_000
NUM_TRANSACTIONS = 1_500_000
NUM_SESSIONS = 3_000_000

# =========================
# HELPER: HEAVY USER DISTRIBUTION
# =========================
def generate_skewed_ids(n_ids, size):
    weights = np.random.zipf(2, n_ids)
    weights = weights / weights.sum()
    return np.random.choice(np.arange(1, n_ids + 1), size=size, p=weights)

# =========================
# 1. CUSTOMERS
# =========================
print("📦 Generating customers...")

email_domains = ["gmail.com", "yahoo.com", "hotmail.com", "outlook.com"]

customers = []

for i in tqdm(range(1, NUM_CUSTOMERS + 1), desc="Customers"):
    name = fake.name()

    # Use email domains
    domain = random.choice(email_domains)
    local_part = name.lower().replace(" ", ".")
    email = f"{local_part}@{domain}"
    country = "Nigeria" if random.random() > 0.03 else None
    created_at = fake.date_time_between(start_date='-3y', end_date='-6m')

    customers.append([i, name, email, country, created_at])

df_customers = pd.DataFrame(customers, columns=[
    "customer_id", "name", "email", "country", "created_at"
])

# Add duplicates
df_customers = pd.concat([df_customers, df_customers.sample(frac=0.01)])

df_customers.to_csv("src_customers.csv", index=False)

print("✅ Customers saved")


# =========================
# 2. BILLING TRANSACTIONS
# =========================
print("💳 Generating transactions... (this will take time)")

customer_ids = generate_skewed_ids(NUM_CUSTOMERS, NUM_TRANSACTIONS)

transactions = []

for i in tqdm(range(1, NUM_TRANSACTIONS + 1), desc="Transactions"):
    cust_id = int(customer_ids[i - 1])

    base_amount = np.random.exponential(scale=2000)
    if random.random() < 0.1:
        base_amount *= 10

    amount = round(base_amount, 2)

    if random.random() < 0.03:
        amount = None

    currency = random.choice(["NGN", "ngn", "Naira", None])
    tx_time = fake.date_time_between(start_date='-1y', end_date='now')

    transactions.append([
        i,
        cust_id,
        amount,
        currency,
        tx_time
    ])

# Inject duplicates
print("🔁 Injecting duplicates...")
transactions += random.sample(transactions, int(0.02 * NUM_TRANSACTIONS))

df_transactions = pd.DataFrame(transactions, columns=[
    "transaction_id", "customer_id", "amount", "currency", "transaction_date"
])

df_transactions.to_csv("src_billing_transactions.csv", index=False)

print("✅ Transactions saved")


# =========================
# 3. NETWORK SESSIONS
# =========================
print("🌐 Generating sessions... (largest step)")

customer_ids_sessions = generate_skewed_ids(NUM_CUSTOMERS, NUM_SESSIONS)

sessions = []

for i in tqdm(range(1, NUM_SESSIONS + 1), desc="Sessions"):
    cust_id = int(customer_ids_sessions[i - 1])

    start = fake.date_time_between(start_date='-1y', end_date='now')

    if random.random() < 0.7:
        duration = random.randint(10, 300)
    elif random.random() < 0.9:
        duration = random.randint(300, 1800)
    else:
        duration = random.randint(1800, 7200)

    end = start + pd.Timedelta(seconds=duration)

    if random.random() < 0.02:
        end = start - pd.Timedelta(seconds=random.randint(1, 300))

    data_used = duration * random.uniform(0.01, 0.2)

    if random.random() < 0.01:
        data_used *= 50

    if random.random() < 0.02:
        data_used = None

    sessions.append([
        i,
        cust_id,
        start,
        end,
        round(data_used, 2) if data_used else None
    ])

# Inject duplicates
print("🔁 Injecting session duplicates...")
sessions += random.sample(sessions, int(0.02 * NUM_SESSIONS))

df_sessions = pd.DataFrame(sessions, columns=[
    "session_id", "customer_id", "start_time", "end_time", "data_used_mb"
])

df_sessions.to_csv("src_network_sessions.csv", index=False)

print("✅ Sessions saved")


# =========================
# FINAL STATUS
# =========================
print("\n🔥 DATA GENERATION COMPLETE")
print("Files created:")
print("- src_customers.csv")
print("- src_billing_transactions.csv")
print("- src_network_sessions.csv")