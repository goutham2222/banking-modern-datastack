import os
import json
import requests
from dotenv import load_dotenv

# -----------------------------
# Load environment variables
# -----------------------------
load_dotenv()

# -----------------------------
# Build connector JSON in memory
# -----------------------------
connector_config = {
    "name": "postgres-connector",
    "config": {
        "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
        "database.hostname": os.getenv("POSTGRES_HOST"),
        "database.port": os.getenv("POSTGRES_PORT"),
        "database.user": os.getenv("POSTGRES_USER"),
        "database.password": os.getenv("POSTGRES_PASSWORD"),
        "database.dbname": os.getenv("POSTGRES_DB"),
        "topic.prefix": "banking_server",
        "table.include.list": "public.customers,public.accounts,public.transactions,public.locations,public.merchants",
        "plugin.name": "pgoutput",
        "slot.name": "banking_slot_final",
        "publication.name": "banking_pub_final",
        "tombstones.on.delete": "false",
        "decimal.handling.mode": "double",
    },
}

# -----------------------------
# Send request to Debezium Connect
# -----------------------------
# Tip: Use the DELETE method first if you need to recreate an existing connector with new config
url = "http://localhost:8083/connectors"
headers = {"Content-Type": "application/json"}

# Optional: requests.delete(f"{url}/postgres-connector") 

response = requests.post(url, headers=headers, data=json.dumps(connector_config))

# -----------------------------
# Debug/Output
# -----------------------------
if response.status_code == 201:
    print("✅ Connector created successfully!")
elif response.status_code == 409:
    print("⚠️ Connector already exists. You may need to delete and recreate it to apply changes.")
else:
    print(f"❌ Failed to create connector ({response.status_code}): {response.text}")