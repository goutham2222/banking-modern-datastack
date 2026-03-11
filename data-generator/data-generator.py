import os
import time
import random
from decimal import Decimal
from datetime import datetime, timedelta
import psycopg2
from faker import Faker
from dotenv import load_dotenv

load_dotenv()
fake = Faker()

# --- Configuration ---
INITIAL_POOL_CUSTOMERS = 500  
NEW_CUSTOMER_CHANCE = 0.03    
NEW_ACCOUNT_CHANCE = 0.08     
TRANSACTIONS_PER_TICK = 35    
SLEEP_TIME = 2

# Categorical Data for Power BI Slicing
INCOME_CATS = ['Low', 'Medium', 'High', 'Ultra High']
EDU_LEVELS = ['High School', 'Bachelors', 'Masters', 'PhD']
MARITAL_STATUSES = ['Single', 'Married', 'Divorced', 'Widowed']
EMP_STATUSES = ['Employed', 'Self-Employed', 'Unemployed', 'Retired']

# Expanded Merchants (15-20 per category for rich distribution)
EXTENDED_MERCHANTS = {
    'Retail': ['Amazon', 'Walmart', 'Target', 'Apple Store', 'Best Buy', 'Home Depot', 'Lowes', 'Costco', 'Nike', 'Zara', 'H&M', 'IKEA', 'Macy\'s', 'Nordstrom', 'Sephora', 'Gap', 'Old Navy', 'Lululemon', 'Dick\'s Sporting Goods', 'Williams-Sonoma'],
    'Grocery': ['Kroger', 'Whole Foods', 'Trader Joes', 'Safeway', 'Publix', 'Albertsons', 'Aldi', 'Wegmans', 'Sprouts', 'HEB', 'Meijer', 'ShopRite', 'Vons', 'Food Lion', 'Harris Teeter', 'Stop & Shop', 'Hy-Vee', 'WinCo Foods'],
    'Entertainment': ['Netflix', 'Steam', 'Regal Cinemas', 'AMC Theatres', 'Cinemark', 'Disney+', 'Spotify', 'PlayStation Network', 'Xbox Live', 'Nintendo eShop', 'Hulu', 'Paramount+', 'StubHub', 'Ticketmaster', 'Dave & Busters', 'Topgolf', 'Six Flags', 'SeaWorld', 'Madison Square Garden'],
    'Utilities': ['Verizon', 'AT&T', 'T-Mobile', 'Duke Energy', 'Comcast', 'Waste Management', 'Sparkle Car Wash', 'Mr. Clean Car Wash', 'Pacific Gas & Electric', 'National Grid', 'Southern Company', 'NextEra Energy', 'American Water', 'Republic Services', 'Cox Communications', 'Charter Spectrum', 'Consolidated Edison', 'Xcel Energy'],
    'Luxury': ['Tesla', 'Rolex', 'Gucci', 'Louis Vuitton', 'Mercedes-Benz', 'BMW', 'Porsche', 'Prada', 'Hermes', 'Dyson', 'Sub-Zero', 'Tiffany & Co.', 'Cartier', 'Ferrari', 'Lamborghini', 'Lexus', 'Carrier AC', 'Trane Technologies', 'Bose', 'Bang & Olufsen']
}

def get_db_connection():
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST"),
        port=os.getenv("POSTGRES_PORT"),
        dbname=os.getenv("POSTGRES_DB"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
    )

def is_payday():
    """Logic for 1st, 15th, or last business day of the month."""
    today = datetime.now()
    if today.day in [1, 15]: return True
    
    last_day = (today.replace(day=28) + timedelta(days=4)).replace(day=1) - timedelta(days=1)
    offset = max(0, (last_day.weekday() - 4)) # Adjust if last day is Sat/Sun
    last_biz_day = last_day - timedelta(days=offset)
    
    return today.date() == last_biz_day.date()

def seed_static_data(cur):
    """Prevents duplicate seeds on script restart."""
    cur.execute("SELECT COUNT(*) FROM merchants")
    if cur.fetchone()[0] == 0:
        print("🌱 Seeding static merchant data...")
        for cat, names in EXTENDED_MERCHANTS.items():
            for name in names:
                cur.execute("INSERT INTO merchants (name, category) VALUES (%s, %s) ON CONFLICT DO NOTHING", (name, cat))
    
    cur.execute("SELECT COUNT(*) FROM locations")
    if cur.fetchone()[0] == 0:
        print("📍 Seeding static location data...")
        for _ in range(100):
            cur.execute("""
                INSERT INTO locations (zip_code, city, state_code, state_name) 
                VALUES (%s, %s, %s, %s) ON CONFLICT DO NOTHING""", 
                (fake.unique.zipcode(), fake.city(), fake.state_abbr(), fake.state()))
    else:
        print("✅ Static data already exists. Skipping seed.")

def create_customer(cur):
    cur.execute("SELECT zip_code FROM locations ORDER BY RANDOM() LIMIT 1")
    zip_code = cur.fetchone()[0]
    
    birth_date = fake.date_of_birth(minimum_age=20, maximum_age=85)
    income = random.choice(INCOME_CATS)
    # Estimated Net Worth logic based on income category
    net_worth = Decimal(random.randint(5000, 2000000)) if income in ['High', 'Ultra High'] else Decimal(random.randint(100, 15000))
    
    cur.execute(
        """INSERT INTO customers (first_name, last_name, email, birth_date, address, zip_code, 
           marital_status, education_level, income_category, estimated_net_worth, employment_status) 
           VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) RETURNING id""",
        (fake.first_name(), fake.last_name(), fake.unique.email(), birth_date, fake.street_address(), 
         zip_code, random.choice(MARITAL_STATUSES), random.choice(EDU_LEVELS), income, net_worth, random.choice(EMP_STATUSES))
    )
    c_id = cur.fetchone()[0]
    create_account(cur, c_id) # Ensure every new customer gets an account
    return c_id

def create_account(cur, customer_id):
    cur.execute(
        "INSERT INTO accounts (customer_id, account_type, balance) VALUES (%s, %s, %s)",
        (customer_id, random.choices(['SAVINGS', 'CHECKING'], weights=[0.3, 0.7])[0], Decimal(random.randint(500, 15000)))
    )

def generate_transactions(cur):
    cur.execute("SELECT id, balance FROM accounts WHERE account_status = 'ACTIVE'")
    active_accounts = cur.fetchall()
    if not active_accounts: return

    cur.execute("SELECT id FROM merchants")
    merchants = [m[0] for m in cur.fetchall()]
    
    payday = is_payday()

    for _ in range(TRANSACTIONS_PER_TICK):
        acc_id, balance = random.choice(active_accounts)
        # Weighted types: 80% Withdrawal, 15% Deposit, 5% Transfer
        t_type = random.choices(['WITHDRAWAL', 'TRANSFER', 'DEPOSIT'], weights=[0.80, 0.05, 0.15])[0]
        amount = Decimal(0)
        m_id = None
        rel_acc_id = None

        if payday and random.random() < 0.4:
            t_type = 'SALARY'
            amount = Decimal(random.randint(3000, 15000))
        elif t_type == 'WITHDRAWAL':
            amount = Decimal(random.uniform(5.00, 1200.00)).quantize(Decimal("0.01"))
            m_id = random.choice(merchants)
        elif t_type == 'TRANSFER':
            amount = Decimal(random.uniform(100.00, 2500.00)).quantize(Decimal("0.01"))
            # Explicit recipient selection for transfers
            rel_acc_id = random.choice([a[0] for a in active_accounts if a[0] != acc_id])
        else: # Regular Deposit
            amount = Decimal(random.uniform(50.00, 1500.00)).quantize(Decimal("0.01"))

        is_hv = amount > 8000
        success = False

        # Atomic SQL updates to avoid CheckViolation error
        if t_type in ['WITHDRAWAL', 'TRANSFER']:
            cur.execute(
                "UPDATE accounts SET balance = balance - %s WHERE id = %s AND balance >= %s", 
                (amount, acc_id, amount)
            )
            if cur.rowcount > 0:
                success = True
                if t_type == 'TRANSFER':
                    cur.execute("UPDATE accounts SET balance = balance + %s WHERE id = %s", (amount, rel_acc_id))
        else:
            cur.execute("UPDATE accounts SET balance = balance + %s WHERE id = %s", (amount, acc_id))
            success = True

        if success:
            cur.execute(
                """INSERT INTO transactions (account_id, merchant_id, transaction_type, amount, 
                related_account_id, is_high_value) VALUES (%s, %s, %s, %s, %s, %s)""",
                (acc_id, m_id, t_type, amount, rel_acc_id, is_hv)
            )

if __name__ == "__main__":
    conn = get_db_connection()
    conn.autocommit = True
    cur = conn.cursor()
    
    seed_static_data(cur)
    
    # Check if initial pool exists
    cur.execute("SELECT COUNT(*) FROM customers")
    if cur.fetchone()[0] < INITIAL_POOL_CUSTOMERS:
        print(f"🚀 Initializing bank with {INITIAL_POOL_CUSTOMERS} customers...")
        for _ in range(INITIAL_POOL_CUSTOMERS): 
            create_customer(cur)

    try:
        while True:
            # Random chance for growth
            if random.random() < NEW_CUSTOMER_CHANCE: 
                create_customer(cur)
                print("🆕 New customer joined.")
            
            if random.random() < NEW_ACCOUNT_CHANCE:
                cur.execute("SELECT id FROM customers ORDER BY RANDOM() LIMIT 1")
                create_account(cur, cur.fetchone()[0])
                print("🏦 New account opened for existing customer.")

            generate_transactions(cur)
            print(f"🏦 Ticked at {datetime.now().strftime('%H:%M:%S')}")
            time.sleep(SLEEP_TIME)
    except KeyboardInterrupt:
        print("\nStopping simulation...")
        cur.close()
        conn.close()