from faker import Faker
import psycopg2
from dotenv import load_dotenv
from decimal import Decimal
import random
import sys
import os
import time
import argparse

load_dotenv()

# --------------------------------
# Defining variables
# --------------------------------

# Variable Constraints
number_of_customers = 15
accounts_per_customer = 3
number_of_transactions = 75
max_transaction_amount = 1700
currency = 'USD'

# Initial Balance
min_initial_balance = Decimal('10.00')
max_initial_balance = Decimal('1700.00')

# Loop Configuration
sleep = 2
loop = True

# CLI override (run once mode)
parser = argparse.ArgumentParser(description="Run fake data generator")
parser.add_argument("--once", action="store_true", help="Run a single iteration and exit")
args = parser.parse_args()
looping = not args.once

# -----------------------------
# Helper function using Faker
# -----------------------------
fake = Faker()

def random_amount(min_val: Decimal, max_val: Decimal) -> Decimal:
    range_cents = int((max_val - min_val) * 100)
    random_cents = random.randint(0, range_cents)
    return (min_val + Decimal(random_cents) / 100).quantize(Decimal("0.01"))

# --------------------------------
# Establishing Postgres using psycopg2
# --------------------------------

conn = psycopg2.connect(
    host = os.getenv("POSTGRES_HOST"),
    port = os.getenv("POSTGRES_PORT"),
    dbname = os.getenv("POSTGRES_DB"),
    user = os.getenv("POSTGRES_USER"),
    password = os.getenv("POSTGRES_PASSWORD"),
)
conn.autocommit = False
cur = conn.cursor()

# ---------------------------------------------
# Generating Records for each table (Runs once)
# ---------------------------------------------

def generate_data():
    customers = []
    # Generate records for customers table
    for _ in range(number_of_customers):
        first_name = fake.first_name()
        last_name = fake.last_name()
        email = fake.email()

        cur.execute(
            "INSERT INTO customers (first_name, last_name, email) VALUES (%s, %s, %s) RETURNING id",
            (first_name, last_name, email),
        )
        customer_id = cur.fetchone()[0]
        customers.append(customer_id)

    if not customers:
        print("No customers were created. Skipping accounts and transactions.")
        return

    # Generate records for accounts table
    accounts = []
    for customer_id in customers:
        for _ in range(accounts_per_customer):
            account_type = random.choice(['SAVINGS', 'CHECKING'])
            initial_balance = random_amount(min_initial_balance, max_initial_balance)
            cur.execute(
                "INSERT INTO accounts (customer_id, account_type, balance, currency) VALUES (%s, %s, %s, %s) RETURNING id",
                (customer_id, account_type, initial_balance, currency),
            )
            account_id = cur.fetchone()[0]
            accounts.append(account_id)

    if not accounts:
        print("No accounts were created. Skipping transaction generation.")
        return

    # Generate records for transactions table
    transaction_types = ['DEPOSIT', 'WITHDRAWAL', 'TRANSFER']
    for _ in range(number_of_transactions):
        account_id = random.choice(accounts)
        transaction_type = random.choice(transaction_types)
        amount = random_amount(Decimal("1.00"), Decimal(str(max_transaction_amount)))
        related_account = None

        if transaction_type == 'TRANSFER' and len(accounts) > 1:
            related_account = random.choice([a for a in accounts if a != account_id])

        cur.execute(
            "INSERT INTO transactions (account_id, transaction_type, amount, related_account_id, status) VALUES (%s, %s, %s, %s, 'COMPLETED')",
            (account_id, transaction_type, amount, related_account),
        )

    print(f"✅ Generated {len(customers)} customers, {len(accounts)} accounts, {number_of_transactions} transactions.")

# -----------------------------
# Actual loop
# -----------------------------

try:
    itr = 0
    while True:
        itr += 1
        print(f"\n--- Iteration {itr} started ---")
        generate_data()
        conn.commit()
        print(f"--- Iteration {itr} finished ---")
        if not looping:
            break
        time.sleep(sleep)

except KeyboardInterrupt:
    print("\nInterrupted by user. Exiting gracefully...")

finally:
    cur.close()
    conn.close()
    sys.exit(0)