from faker import Faker
import psycopg2
from dotenv import load_dotenv
from decimal import Decimal
import random
import sys
import os
import time

load_dotenv()

# --- Configuration ---
INITIAL_POOL_CUSTOMERS = 100
NEW_CUSTOMER_CHANCE = 0.10  
NEW_ACCOUNT_CHANCE = 0.20   
TRANSACTIONS_PER_TICK = 15  
SLEEP_TIME = 3              

fake = Faker()

def get_db_connection():
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST"),
        port=os.getenv("POSTGRES_PORT"),
        dbname=os.getenv("POSTGRES_DB"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
    )

def random_amount(min_val: Decimal, max_val: Decimal) -> Decimal:
    range_cents = int((max_val - min_val) * 100)
    # Ensure we don't return 0 by adding a small offset if needed or choosing again
    val = (min_val + Decimal(random.randint(0, range_cents)) / 100).quantize(Decimal("0.01"))
    return val

def create_customer(cur):
    cur.execute(
        "INSERT INTO customers (first_name, last_name, email) VALUES (%s, %s, %s) RETURNING id",
        (fake.first_name(), fake.last_name(), fake.email()),
    )
    c_id = cur.fetchone()[0]
    create_account(cur, c_id)
    return c_id

def create_account(cur, customer_id):
    account_type = random.choice(['SAVINGS', 'CHECKING'])
    balance = random_amount(Decimal('100.00'), Decimal('2000.00'))
    cur.execute(
        "INSERT INTO accounts (customer_id, account_type, balance, currency) VALUES (%s, %s, %s, 'USD')",
        (customer_id, account_type, balance),
    )

def generate_transactions(cur):
    cur.execute("SELECT id FROM accounts")
    account_ids = [r[0] for r in cur.fetchall()]
    if not account_ids: return

    for _ in range(TRANSACTIONS_PER_TICK):
        acc_id = random.choice(account_ids)
        
        # FIX: Ensure change is NEVER zero to satisfy Postgres CHECK constraint
        change = Decimal('0.00')
        while change == Decimal('0.00'):
            change = random_amount(Decimal("-50.00"), Decimal("100.00"))
        
        # Balance Update
        cur.execute("UPDATE accounts SET balance = balance + %s WHERE id = %s AND (balance + %s) >= 0", (change, acc_id, change))
        
        # Only log the transaction if the UPDATE actually affected a row (balance didn't go negative)
        if cur.rowcount > 0:
            cur.execute(
                "INSERT INTO transactions (account_id, transaction_type, amount, status) VALUES (%s, %s, %s, 'COMPLETED')",
                (acc_id, 'ADJUSTMENT', abs(change)),
            )

if __name__ == "__main__":
    conn = get_db_connection()
    conn.autocommit = True
    cur = conn.cursor()
    
    try:
        cur.execute("SELECT COUNT(*) FROM customers")
        if cur.fetchone()[0] == 0:
            print(f"🚀 Creating initial pool of {INITIAL_POOL_CUSTOMERS} customers...")
            for _ in range(INITIAL_POOL_CUSTOMERS):
                create_customer(cur)

        print(f"🏦 Bank is active. Ticking every {SLEEP_TIME}s...")
        while True:
            if random.random() < NEW_CUSTOMER_CHANCE:
                create_customer(cur)
                print("🆕 New customer added.")
            if random.random() < NEW_ACCOUNT_CHANCE:
                cur.execute("SELECT id FROM customers")
                all_cust = [r[0] for r in cur.fetchall()]
                create_account(cur, random.choice(all_cust))
                print("🏦 New account opened.")

            generate_transactions(cur)
            time.sleep(SLEEP_TIME)

    except KeyboardInterrupt:
        print("\nStopping simulation...")
    finally:
        cur.close()
        conn.close()