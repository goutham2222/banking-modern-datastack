from faker import faker
import psycopg2
from dotenv import load_dotenv

# --------------------------------
# Establishing Postgres Connection
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
