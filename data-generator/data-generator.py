import os
import time
import random
from decimal import Decimal
from datetime import datetime, timedelta
import psycopg2
from faker import Faker
from dotenv import load_dotenv

# Load environment variables for DB credentials
load_dotenv()
fake = Faker()

# --- Configuration ---
INITIAL_POOL_CUSTOMERS = 1000
NEW_CUSTOMER_CHANCE = 0.08
NEW_ACCOUNT_CHANCE = 0.13
TRANSACTIONS_PER_TICK = 35
SLEEP_TIME = 2
FAILURE_CHANCE = 0.05  # 5% probability of a technical/system failure

# Categorical data for rich Power BI slicing and demographic analysis
INCOME_CATS = ['Low', 'Medium', 'High', 'Ultra High']
EDU_LEVELS = ['High School', 'Bachelors', 'Masters', 'PhD']
MARITAL_STATUSES = ['Single', 'Married', 'Divorced', 'Widowed']
EMP_STATUSES = ['Employed', 'Self-Employed', 'Unemployed', 'Retired']

# Expanded Merchants: Used to generate realistic retail vs. luxury spend patterns
EXTENDED_MERCHANTS = {
    'Retail': ['Amazon', 'Walmart', 'Target', 'Apple Store', 'Best Buy', 'Home Depot', 'Lowes', 'Costco', 'Nike', 'Zara', 'H&M', 'IKEA', 'Macy\'s', 'Nordstrom', 'Sephora', 'Gap', 'Old Navy', 'Lululemon', 'Dick\'s Sporting Goods', 'Williams-Sonoma'],
    'Grocery': ['Kroger', 'Whole Foods', 'Trader Joes', 'Safeway', 'Publix', 'Albertsons', 'Aldi', 'Wegmans', 'Sprouts', 'HEB', 'Meijer', 'ShopRite', 'Vons', 'Food Lion', 'Harris Teeter', 'Stop & Shop', 'Hy-Vee', 'WinCo Foods'],
    'Entertainment': ['Netflix', 'Steam', 'Regal Cinemas', 'AMC Theatres', 'Cinemark', 'Disney+', 'Spotify', 'PlayStation Network', 'Xbox Live', 'Nintendo eShop', 'Hulu', 'Paramount+', 'StubHub', 'Ticketmaster', 'Dave & Busters', 'Topgolf', 'Six Flags', 'SeaWorld', 'Madison Square Garden'],
    'Utilities': ['Verizon', 'AT&T', 'T-Mobile', 'Duke Energy', 'Comcast', 'Waste Management', 'Sparkle Car Wash', 'Mr. Clean Car Wash', 'Pacific Gas & Electric', 'National Grid', 'Southern Company', 'NextEra Energy', 'American Water', 'Republic Services', 'Cox Communications', 'Charter Spectrum', 'Consolidated Edison', 'Xcel Energy'],
    'Luxury': ['Tesla', 'Rolex', 'Gucci', 'Louis Vuitton', 'Mercedes-Benz', 'BMW', 'Porsche', 'Prada', 'Hermes', 'Dyson', 'Sub-Zero', 'Tiffany & Co.', 'Cartier', 'Ferrari', 'Lamborghini', 'Lexus', 'Carrier AC', 'Trane Technologies', 'Bose', 'Bang & Olufsen']
}

def get_db_connection():
    """Handles connection to Postgres with basic retry logic for stability."""
    try:
        return psycopg2.connect(
            host=os.getenv("POSTGRES_HOST"),
            port=os.getenv("POSTGRES_PORT"),
            dbname=os.getenv("POSTGRES_DB"),
            user=os.getenv("POSTGRES_USER"),
            password=os.getenv("POSTGRES_PASSWORD"),
        )
    except Exception as e:
        print(f"❌ Connection failed: {e}. Retrying in 5s...")
        time.sleep(5)
        return get_db_connection()

def is_payday():
    """Returns True if today is a standard banking payday (1st, 15th, or last biz day)."""
    today = datetime.now()
    if today.day in [1, 15]: 
        return True
    last_day = (today.replace(day=28) + timedelta(days=4)).replace(day=1) - timedelta(days=1)
    offset = max(0, (last_day.weekday() - 4))
    return today.date() == (last_day - timedelta(days=offset)).date()

def seed_static_data(cur):
    """
    Populates standard merchants, locations, and system virtual merchants.
    Virtual merchants (IDs < 0) are required to satisfy Foreign Key constraints for non-retail transfers.
    """
    # 1. Standard Retail Merchants
    cur.execute("SELECT COUNT(*) FROM merchants WHERE id > 0")
    if cur.fetchone()[0] == 0:
        print("🌱 Seeding standard merchants...")
        for cat, names in EXTENDED_MERCHANTS.items():
            for name in names:
                cur.execute("INSERT INTO merchants (name, category) VALUES (%s, %s) ON CONFLICT DO NOTHING", (name, cat))
    
    # 2. Virtual System Merchants (Fixed FK Violations)
    # These IDs must exist in 'merchants' table so 'transactions' table can reference them.
    virtual_merchants = [
        ( 0, 'Unknown/Other', 'Other'),
        (-1, 'Direct Deposit', 'Inbound Revenue'),
        (-3, 'Zelle Network', 'P2P Transfer'),
        (-4, 'ACH Clearing House', 'Bank Transfer'),
        (-5, 'Wire Transfer', 'High-Value Transfer')
    ]
    for v_id, v_name, v_cat in virtual_merchants:
        cur.execute("""
            INSERT INTO merchants (id, name, category) 
            VALUES (%s, %s, %s) 
            ON CONFLICT (id) DO NOTHING
        """, (v_id, v_name, v_cat))

    # 3. Geo-locations
    cur.execute("SELECT COUNT(*) FROM locations")
    if cur.fetchone()[0] == 0:
        print("📍 Seeding locations...")
        for _ in range(100):
            cur.execute("INSERT INTO locations (zip_code, city, state_code, state_name) VALUES (%s, %s, %s, %s) ON CONFLICT DO NOTHING", 
                (fake.zipcode(), fake.city(), fake.state_abbr(), fake.state()))

def create_customer(cur, conn):
    """
    Generates a new customer with starting balances weighted by income category.
    Includes a retry loop to handle potential 'UniqueViolation' errors from email collisions.
    """
    while True:
        try:
            # 1. Fetch a random zip code for the customer profile
            cur.execute("SELECT zip_code FROM locations ORDER BY RANDOM() LIMIT 1")
            zip_code = cur.fetchone()[0]
            
            # 2. Determine wealth tier based on weighted probability
            income = random.choices(INCOME_CATS, weights=[0.5, 0.35, 0.12, 0.03])[0]
            
            if income == 'Ultra High':
                net_worth, start_bal = Decimal(random.randint(5000000, 50000000)), Decimal(random.randint(100000, 1000000))
            elif income == 'High':
                net_worth, start_bal = Decimal(random.randint(500000, 5000000)), Decimal(random.randint(20000, 150000))
            else:
                net_worth = Decimal(random.randint(500, 100000))
                start_bal = Decimal(random.randint(1000, 45000)) if random.random() < 0.1 else Decimal(random.randint(50, 5000))

            # 3. Attempt the insert
            cur.execute("""
                INSERT INTO customers (first_name, last_name, email, birth_date, address, zip_code, 
                marital_status, education_level, income_category, estimated_net_worth, employment_status) 
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) RETURNING id""",
                (
                    fake.first_name(), 
                    fake.last_name(), 
                    fake.unique.email(), 
                    fake.date_of_birth(minimum_age=20, maximum_age=85), 
                    fake.street_address(), 
                    zip_code, 
                    random.choice(MARITAL_STATUSES), 
                    random.choice(EDU_LEVELS), 
                    income, 
                    net_worth, 
                    random.choice(EMP_STATUSES)
                )
            )
            
            # 4. If successful, create their account and return
            c_id = cur.fetchone()[0]
            create_account(cur, c_id, start_bal)
            return c_id

        except psycopg2.errors.UniqueViolation:
            conn.rollback() # Required to reset connection after a failed constraint check
            continue

def create_account(cur, customer_id, balance=None):
    if balance is None: 
        balance = Decimal(random.randint(500, 5000))
    cur.execute("INSERT INTO accounts (customer_id, account_type, balance) VALUES (%s, %s, %s)",
                (customer_id, random.choices(['SAVINGS', 'CHECKING'], weights=[0.3, 0.7])[0], balance))

def generate_transactions(conn, cur):
    """
    Main transaction logic. Implements three-tier status system (COMPLETED, DECLINED, FAILED).
    Maps Zelle/ACH/Wire to virtual IDs to distinguish them from standard merchant retail spend.
    """
    cur.execute("SELECT a.id, a.balance, c.income_category FROM accounts a JOIN customers c ON a.customer_id = c.id WHERE a.account_status = 'ACTIVE'")
    active_accounts = cur.fetchall()
    
    # We only want real merchants (ID > 0) for standard 'PURCHASE' types
    cur.execute("SELECT id, category FROM merchants WHERE id > 0")
    merchants = cur.fetchall()
    payday = is_payday()

    for _ in range(TRANSACTIONS_PER_TICK):
        acc_id, balance, income_cat = random.choice(active_accounts)
        t_type = random.choices(['PURCHASE', 'WITHDRAWAL', 'ZELLE', 'ACH', 'WIRE', 'DEPOSIT'], weights=[0.60, 0.05, 0.10, 0.10, 0.02, 0.13])[0]
        amount, m_id, rel_acc_id = Decimal(0), None, None

        # 1. Salary Logic: Inbound funds marked with virtual ID -1
        if payday and random.random() < 0.4:
            t_type, amount, m_id = 'SALARY', Decimal(random.randint(3000, 15000)) if random.random() > 0.03 else Decimal(random.randint(200, 2000)), -1
        
        # 2. Merchant Purchases: Validated against income categories
        elif t_type == 'PURCHASE':
            m_id, m_cat = random.choice(merchants)
            if m_cat == 'Luxury' and income_cat not in ['High', 'Ultra High']:
                m_id, m_cat = random.choice([m for m in merchants if m[1] == 'Retail'])
            
            if m_cat == 'Luxury':
                amount = Decimal(random.uniform(5000, 45000))
            else:
                amount = Decimal(random.uniform(8001, 20000)) if random.random() < 0.02 else Decimal(random.uniform(5, 800))
        
        # 3. Withdrawal Logic
        elif t_type == 'WITHDRAWAL':
            amount = Decimal(random.uniform(20, 800))
            
        # 4. Digital Rails (Zelle, ACH, Wire): Assigned specific virtual IDs for analytics
        elif t_type in ['ZELLE', 'ACH', 'WIRE']:
            m_id = -3 if t_type == 'ZELLE' else -4 if t_type == 'ACH' else -5
            
            if t_type == 'ZELLE': 
                amount = Decimal(random.uniform(10, 1500))
            elif t_type == 'ACH': 
                amount = Decimal(random.uniform(2000, 10000))
            else: 
                amount = Decimal(random.uniform(10001, 50000)) # Wire Transfer
            
            if random.random() >= 0.5: 
                rel_acc_id = random.choice([a[0] for a in active_accounts if a[0] != acc_id])
        
        # 5. Deposits: Marked with virtual ID -1
        elif t_type == 'DEPOSIT':
            amount, m_id = Decimal(random.uniform(50, 2000)), -1

        amount = amount.quantize(Decimal("0.01"))
        is_hv = (amount > 8000)
        status = 'FAILED' if random.random() < FAILURE_CHANCE else 'COMPLETED'
        
        try:
            conn.rollback() # Ensure no partial transaction state remains
            
            if t_type in ['PURCHASE', 'WITHDRAWAL', 'ZELLE', 'ACH', 'WIRE']:
                if status == 'COMPLETED':
                    cur.execute("UPDATE accounts SET balance = balance - %s WHERE id = %s AND balance >= %s", (amount, acc_id, amount))
                    if cur.rowcount > 0:
                        # SUCCESS: Balances adjusted and recipient credited if internal
                        if rel_acc_id: 
                            cur.execute("UPDATE accounts SET balance = balance + %s WHERE id = %s", (amount, rel_acc_id))
                        cur.execute("INSERT INTO transactions (account_id, merchant_id, transaction_type, amount, related_account_id, status, is_high_value) VALUES (%s, %s, %s, %s, %s, %s, %s)",
                                    (acc_id, m_id, t_type, amount, rel_acc_id, 'COMPLETED', is_hv))
                    else:
                        # DECLINED: Insufficient funds
                        cur.execute("INSERT INTO transactions (account_id, merchant_id, transaction_type, amount, related_account_id, status, is_high_value) VALUES (%s, %s, %s, %s, %s, %s, %s)",
                                    (acc_id, m_id, t_type, amount, rel_acc_id, 'DECLINED', is_hv))
                else:
                    # FAILED: Technical failure recorded with no account impact
                    cur.execute("INSERT INTO transactions (account_id, merchant_id, transaction_type, amount, related_account_id, status, is_high_value) VALUES (%s, %s, %s, %s, %s, %s, %s)",
                                (acc_id, m_id, t_type, amount, rel_acc_id, 'FAILED', is_hv))
            else:
                # Inbound logic (Salary/Deposit)
                if status == 'COMPLETED':
                    cur.execute("UPDATE accounts SET balance = balance + %s WHERE id = %s", (amount, acc_id))
                    cur.execute("INSERT INTO transactions (account_id, merchant_id, transaction_type, amount, related_account_id, status, is_high_value) VALUES (%s, %s, %s, %s, %s, %s, %s)",
                                (acc_id, m_id, t_type, amount, rel_acc_id, 'COMPLETED', is_hv))
                else:
                    cur.execute("INSERT INTO transactions (account_id, merchant_id, transaction_type, amount, related_account_id, status, is_high_value) VALUES (%s, %s, %s, %s, %s, %s, %s)",
                                (acc_id, m_id, t_type, amount, rel_acc_id, 'FAILED', is_hv))
            
            conn.commit() # Atomic write of account state and log
        except Exception as e:
            conn.rollback()
            print(f"⚠️ Simulation Error: {e}")

if __name__ == "__main__":
    conn = get_db_connection()
    cur = conn.cursor()
    seed_static_data(cur)
    conn.commit()
    
    cur.execute("SELECT COUNT(*) FROM customers")
    if cur.fetchone()[0] < INITIAL_POOL_CUSTOMERS:
        print(f"🚀 Initializing bank with {INITIAL_POOL_CUSTOMERS} customers...")
        for _ in range(INITIAL_POOL_CUSTOMERS): 
            create_customer(cur, conn); 
            conn.commit()
        
    try:
        while True:
            # Randomly add new customers to simulate organic bank growth
            if random.random() < NEW_CUSTOMER_CHANCE: 
                create_customer(cur, conn); 
                conn.commit(); 
                print("🆕 Customer joined.")

            # New Accounts for Existing Customers
            if random.random() < NEW_ACCOUNT_CHANCE:
                cur.execute("SELECT id FROM customers ORDER BY RANDOM() LIMIT 1")
                random_customer_id = cur.fetchone()[0]
                create_account(cur, random_customer_id)
                conn.commit()
                print(f"💳 Existing customer {random_customer_id} opened a new account.")
            
            generate_transactions(conn, cur)
            print(f"🏦 Ticked at {datetime.now().strftime('%H:%M:%S')}")
            time.sleep(SLEEP_TIME)
    except KeyboardInterrupt:
        cur.close(); 
        conn.close()