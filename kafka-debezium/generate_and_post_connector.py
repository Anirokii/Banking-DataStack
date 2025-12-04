import os
import json
import requests
from dotenv import load_dotenv
import time

# -----------------------------
# Load environment variables
# -----------------------------
load_dotenv()

# -----------------------------
# Configuration
# -----------------------------
# IMPORTANT: Use container name 'postgres' instead of 'localhost'
# because Debezium Connect runs inside Docker and needs to reach
# the postgres container via the Docker network
POSTGRES_HOST_DOCKER = "postgres"  # Container name in docker-compose
POSTGRES_PORT_DOCKER = "5432"      # Internal port inside Docker network

# For the Connect API, we use localhost because we're calling it from Windows
CONNECT_URL = "http://localhost:8083"

# -----------------------------
# Helper function to check Connect health
# -----------------------------
def wait_for_connect(max_retries=10, delay=3):
    """Wait for Kafka Connect to be ready"""
    print("🔌 Checking if Kafka Connect is ready...")
    for attempt in range(max_retries):
        try:
            response = requests.get(f"{CONNECT_URL}/connectors", timeout=5)
            if response.status_code == 200:
                print("✅ Kafka Connect is ready!")
                return True
        except requests.exceptions.RequestException as e:
            print(f"⏳ Attempt {attempt + 1}/{max_retries}: Connect not ready yet...")
            if attempt < max_retries - 1:
                time.sleep(delay)
    print("❌ Kafka Connect is not responding. Is the container running?")
    return False

# -----------------------------
# Build connector JSON
# -----------------------------
connector_config = {
    "name": "postgres-connector",
    "config": {
        "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
        
        # Database connection (using Docker container name)
        "database.hostname": POSTGRES_HOST_DOCKER,
        "database.port": POSTGRES_PORT_DOCKER,
        "database.user": os.getenv("POSTGRES_USER", "postgres"),
        "database.password": os.getenv("POSTGRES_PASSWORD", "postgres"),
        "database.dbname": os.getenv("POSTGRES_DB", "banking"),
        
        # Kafka topic configuration
        "topic.prefix": "banking_server",
        "table.include.list": "public.customers,public.accounts,public.transactions",
        
        # PostgreSQL specific settings
        "plugin.name": "pgoutput",
        "slot.name": "banking_slot",
        "publication.name": "banking_publication",
        "publication.autocreate.mode": "filtered",
        
        # Data handling
        "tombstones.on.delete": "false",
        "decimal.handling.mode": "double",
        "time.precision.mode": "connect",
        
        # Snapshot settings
        "snapshot.mode": "initial",
        
        # Heartbeat (optional but recommended)
        "heartbeat.interval.ms": "10000",
        "heartbeat.topics.prefix": "__debezium-heartbeat",
    },
}

# -----------------------------
# Check if connector already exists
# -----------------------------
def check_connector_exists(connector_name):
    """Check if connector already exists"""
    try:
        response = requests.get(f"{CONNECT_URL}/connectors/{connector_name}")
        return response.status_code == 200
    except requests.exceptions.RequestException:
        return False

def delete_connector(connector_name):
    """Delete existing connector"""
    try:
        response = requests.delete(f"{CONNECT_URL}/connectors/{connector_name}")
        if response.status_code == 204:
            print(f"🗑️  Deleted existing connector '{connector_name}'")
            time.sleep(2)  # Wait a bit before recreating
            return True
        return False
    except requests.exceptions.RequestException as e:
        print(f"❌ Error deleting connector: {e}")
        return False

# -----------------------------
# Main execution
# -----------------------------
def main():
    print("\n" + "="*60)
    print("🚀 DEBEZIUM CONNECTOR SETUP")
    print("="*60 + "\n")
    
    # Step 1: Wait for Connect to be ready
    if not wait_for_connect():
        return
    
    # Step 2: Check if connector exists
    connector_name = connector_config["name"]
    if check_connector_exists(connector_name):
        print(f"\n⚠️  Connector '{connector_name}' already exists.")
        user_input = input("Do you want to delete and recreate it? (y/n): ").strip().lower()
        if user_input == 'y':
            if not delete_connector(connector_name):
                print("❌ Failed to delete connector. Exiting.")
                return
        else:
            print("ℹ️  Keeping existing connector. Exiting.")
            return
    
    # Step 3: Create connector
    print(f"\n📝 Creating connector '{connector_name}'...")
    print(f"   Database: {POSTGRES_HOST_DOCKER}:{POSTGRES_PORT_DOCKER}")
    print(f"   Database name: {connector_config['config']['database.dbname']}")
    print(f"   Tables: {connector_config['config']['table.include.list']}")
    
    headers = {"Content-Type": "application/json"}
    
    try:
        response = requests.post(
            f"{CONNECT_URL}/connectors",
            headers=headers,
            data=json.dumps(connector_config),
            timeout=10
        )
        
        if response.status_code == 201:
            print("\n✅ Connector created successfully!")
            print("\n📊 Connector details:")
            print(json.dumps(response.json(), indent=2))
            
            # Check connector status
            print("\n🔍 Checking connector status...")
            time.sleep(2)
            status_response = requests.get(f"{CONNECT_URL}/connectors/{connector_name}/status")
            if status_response.status_code == 200:
                status = status_response.json()
                print(f"\n   State: {status['connector']['state']}")
                print(f"   Worker: {status['connector']['worker_id']}")
                
                if status['tasks']:
                    print(f"   Tasks: {len(status['tasks'])}")
                    for i, task in enumerate(status['tasks']):
                        print(f"      Task {i}: {task['state']}")
                else:
                    print("   ⚠️  No tasks yet (may take a few seconds)")
            
        elif response.status_code == 409:
            print("\n⚠️  Connector already exists.")
            
        else:
            print(f"\n❌ Failed to create connector (HTTP {response.status_code})")
            print(f"\nError details:")
            try:
                error = response.json()
                print(json.dumps(error, indent=2))
            except:
                print(response.text)
            
            print("\n💡 Troubleshooting tips:")
            print("   1. Check that Postgres is running: docker ps | findstr postgres")
            print("   2. Check Connect logs: docker-compose logs connect")
            print("   3. Verify Postgres has WAL enabled (wal_level=logical)")
            
    except requests.exceptions.RequestException as e:
        print(f"\n❌ Connection error: {e}")
        print("\n💡 Make sure Kafka Connect container is running:")
        print("   docker ps | findstr connect")

if __name__ == "__main__":
    main()