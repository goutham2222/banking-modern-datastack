from faker import Faker
import psycopg2
from dotenv import load_dotenv
from decimal import Decimal, ROUND_DOWN

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
LOOP = not args.once and DEFAULT_LOOP

# -----------------------------
# Helper function using Faker
# -----------------------------
fake = Faker()

def random_amount(min_val: Decimal, max_val: Decimal) -> Decimal:
    val = Decimal(str(random.uniform(float(min_val), float(max_val))))
    return val.quantize(Decimal("0.01"), rounding=ROUND_DOWN)

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
conn.autocommit = True
cur = conn.cursor()
