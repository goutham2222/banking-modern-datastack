from faker import Faker
import psycopg2
from dotenv import load_dotenv
from decimal import Decimal
import random
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
    return (min_val + Decimal(random.randint(0, range_cents)) / 100).quantize(Decimal("0.01"))

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
    balance = random_amount(Decimal('500.00'), Decimal('5000.00'))
    cur.execute(
        "INSERT INTO accounts (customer_id, account_type, balance, currency) VALUES (%s, %s, %s, 'USD')",
        (customer_id, account_type, balance),
    )

def generate_transactions(cur):
    cur.execute("SELECT id, balance FROM accounts")
    accounts = cur.fetchall()
    if not accounts: return

    for _ in range(TRANSACTIONS_PER_TICK):
        acc_id, current_balance = random.choice(accounts)
        t_type = random.choice(['DEPOSIT', 'WITHDRAWAL', 'TRANSFER'])
        amount = random_amount(Decimal("10.00"), Decimal("500.00"))

        if t_type == 'WITHDRAWAL':
            # Only update if balance is sufficient
            cur.execute(
                "UPDATE accounts SET balance = balance - %s WHERE id = %s AND balance >= %s", 
                (amount, acc_id, amount)
            )
        elif t_type == 'DEPOSIT':
            cur.execute(
                "UPDATE accounts SET balance = balance + %s WHERE id = %s", 
                (amount, acc_id)
            )
        elif t_type == 'TRANSFER':
            # Pick a target account that isn't the source
            other_accounts = [a[0] for a in accounts if a[0] != acc_id]
            if other_accounts:
                target_id = random.choice(other_accounts)
                # Deduct from source
                cur.execute(
                    "UPDATE accounts SET balance = balance - %s WHERE id = %s AND balance >= %s", 
                    (amount, acc_id, amount)
                )
                if cur.rowcount > 0:
                    # Add to target
                    cur.execute("UPDATE accounts SET balance = balance + %s WHERE id = %s", (amount, target_id))
            else:
                continue

        # Log the transaction only if the balance update was successful
        if cur.rowcount > 0:
            cur.execute(
                "INSERT INTO transactions (account_id, transaction_type, amount, status) VALUES (%s, %s, %s, 'COMPLETED')",
                (acc_id, t_type, amount),
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