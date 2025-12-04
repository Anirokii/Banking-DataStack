import time
import psycopg2
from decimal import Decimal, ROUND_DOWN
from faker import Faker
import random
import argparse
import sys
import os
from dotenv import load_dotenv

load_dotenv()

# -----------------------------
# Project configuration
# -----------------------------
NUM_CUSTOMERS = 10
ACCOUNTS_PER_CUSTOMER = 2
NUM_TRANSACTIONS = 50
MAX_TXN_AMOUNT = 1000.00
CURRENCY = "USD"

# Non-zero initial balances
INITIAL_BALANCE_MIN = Decimal("10.00")
INITIAL_BALANCE_MAX = Decimal("1000.00")

# Loop config
DEFAULT_LOOP = True
SLEEP_SECONDS = 2

# CLI override (run once mode)
parser = argparse.ArgumentParser(description="Run fake data generator")
parser.add_argument("--once", action="store_true", help="Run a single iteration and exit")
args = parser.parse_args()
LOOP = not args.once and DEFAULT_LOOP

# -----------------------------
# Helpers
# -----------------------------
fake = Faker()

def random_money(min_val: Decimal, max_val: Decimal) -> Decimal:
    val = Decimal(str(random.uniform(float(min_val), float(max_val))))
    return val.quantize(Decimal("0.01"), rounding=ROUND_DOWN)

# -----------------------------
# Database connection with error handling
# -----------------------------
def get_db_connection():
    """Connect to PostgreSQL with retry logic"""
    max_retries = 5
    retry_delay = 2
    
    for attempt in range(max_retries):
        try:
            print(f"🔌 Attempting to connect to database (attempt {attempt + 1}/{max_retries})...")
            conn = psycopg2.connect(
                host=os.getenv("POSTGRES_HOST", "localhost"),
                port=os.getenv("POSTGRES_PORT", "5440"),
                dbname=os.getenv("POSTGRES_DB", "banking"),
                user=os.getenv("POSTGRES_USER", "postgres"),
                password=os.getenv("POSTGRES_PASSWORD", "postgres"),
            )
            conn.autocommit = True
            print("✅ Connected to database successfully!")
            return conn
        except psycopg2.OperationalError as e:
            print(f"❌ Connection failed: {e}")
            if attempt < max_retries - 1:
                print(f"⏳ Retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)
            else:
                print("🚫 Max retries reached. Exiting.")
                sys.exit(1)

def check_tables_exist(cur):
    """Verify that required tables exist"""
    print("🔍 Checking if tables exist...")
    tables = ['customers', 'accounts', 'transactions']
    for table in tables:
        cur.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = 'public' 
                AND table_name = %s
            );
        """, (table,))
        exists = cur.fetchone()[0]
        if not exists:
            print(f"❌ Table '{table}' does not exist!")
            print("\n🛠️  Please run the init_database.sql script first:")
            print(f"   docker exec -i postgres psql -U postgres -d banking < init_database.sql")
            return False
        else:
            print(f"✅ Table '{table}' exists")
    return True

# Connect to database
conn = get_db_connection()
cur = conn.cursor()

# Check if tables exist
if not check_tables_exist(cur):
    cur.close()
    conn.close()
    sys.exit(1)

# -----------------------------
# Core generation logic (one iteration)
# -----------------------------
def run_iteration():
    customers = []
    try:
        # 1. Generate customers
        print(f"👥 Generating {NUM_CUSTOMERS} customers...")
        for i in range(NUM_CUSTOMERS):
            first_name = fake.first_name()
            last_name = fake.last_name()
            email = fake.unique.email()

            cur.execute(
                "INSERT INTO customers (first_name, last_name, email) VALUES (%s, %s, %s) RETURNING id",
                (first_name, last_name, email),
            )
            customer_id = cur.fetchone()[0]
            customers.append(customer_id)
            if (i + 1) % 10 == 0:
                print(f"   Created {i + 1} customers...")

        # 2. Generate accounts
        print(f"💳 Generating {len(customers) * ACCOUNTS_PER_CUSTOMER} accounts...")
        accounts = []
        for idx, customer_id in enumerate(customers):
            for _ in range(ACCOUNTS_PER_CUSTOMER):
                account_type = random.choice(["SAVINGS", "CHECKING"])
                initial_balance = random_money(INITIAL_BALANCE_MIN, INITIAL_BALANCE_MAX)
                cur.execute(
                    "INSERT INTO accounts (customer_id, account_type, balance, currency) VALUES (%s, %s, %s, %s) RETURNING id",
                    (customer_id, account_type, initial_balance, CURRENCY),
                )
                account_id = cur.fetchone()[0]
                accounts.append(account_id)
            if (idx + 1) % 10 == 0:
                print(f"   Created accounts for {idx + 1} customers...")

        # 3. Generate transactions
        print(f"💸 Generating {NUM_TRANSACTIONS} transactions...")
        txn_types = ["DEPOSIT", "WITHDRAWAL", "TRANSFER"]
        for i in range(NUM_TRANSACTIONS):
            account_id = random.choice(accounts)
            txn_type = random.choice(txn_types)
            amount = round(random.uniform(1, MAX_TXN_AMOUNT), 2)
            related_account = None
            if txn_type == "TRANSFER" and len(accounts) > 1:
                related_account = random.choice([a for a in accounts if a != account_id])

            cur.execute(
                "INSERT INTO transactions (account_id, txn_type, amount, related_account_id, status) VALUES (%s, %s, %s, %s, 'COMPLETED')",
                (account_id, txn_type, amount, related_account),
            )
            if (i + 1) % 20 == 0:
                print(f"   Created {i + 1} transactions...")

        print(f"\n✅ Successfully generated:")
        print(f"   - {len(customers)} customers")
        print(f"   - {len(accounts)} accounts")
        print(f"   - {NUM_TRANSACTIONS} transactions")
        
        # Show some stats
        cur.execute("SELECT COUNT(*) FROM customers")
        total_customers = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM accounts")
        total_accounts = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM transactions")
        total_transactions = cur.fetchone()[0]
        
        print(f"\n📊 Total in database:")
        print(f"   - {total_customers} customers")
        print(f"   - {total_accounts} accounts")
        print(f"   - {total_transactions} transactions")

    except psycopg2.Error as e:
        print(f"\n❌ Database error: {e}")
        raise

# -----------------------------
# Main loop
# -----------------------------
try:
    iteration = 0
    print("\n" + "="*60)
    print("🚀 BANKING DATA GENERATOR STARTED")
    print("="*60)
    print(f"Mode: {'LOOP' if LOOP else 'SINGLE RUN'}")
    print(f"Sleep between iterations: {SLEEP_SECONDS}s" if LOOP else "")
    print("="*60 + "\n")
    
    while True:
        iteration += 1
        print(f"\n{'='*60}")
        print(f"📝 ITERATION {iteration} STARTED")
        print(f"{'='*60}")
        
        run_iteration()
        
        print(f"\n{'='*60}")
        print(f"✅ ITERATION {iteration} FINISHED")
        print(f"{'='*60}\n")
        
        if not LOOP:
            print("🏁 Single run completed. Exiting.")
            break
            
        print(f"⏳ Sleeping for {SLEEP_SECONDS} seconds...")
        time.sleep(SLEEP_SECONDS)

except KeyboardInterrupt:
    print("\n\n" + "="*60)
    print("⚠️  INTERRUPTED BY USER")
    print("="*60)
    print("Exiting gracefully...")

except Exception as e:
    print(f"\n❌ Unexpected error: {e}")
    import traceback
    traceback.print_exc()

finally:
    cur.close()
    conn.close()
    print("\n🔌 Database connection closed.")
    print("👋 Goodbye!\n")
    sys.exit(0)