import os
import boto3
import snowflake.connector
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from datetime import datetime, timedelta

# -------- Configuration depuis Airflow Variables --------
# NOTE: Ne PAS utiliser load_dotenv() dans les DAGs Airflow!
# Les variables doivent être définies via l'UI Airflow ou variables d'environnement du conteneur

def get_config():
    """Get configuration from Airflow Variables or environment variables"""
    return {
        # MinIO Config
        'MINIO_ENDPOINT': Variable.get("MINIO_ENDPOINT", default_var="http://minio:9000"),
        'MINIO_ACCESS_KEY': Variable.get("MINIO_ACCESS_KEY", default_var="minioadmin"),
        'MINIO_SECRET_KEY': Variable.get("MINIO_SECRET_KEY", default_var="minioadmin"),
        'MINIO_BUCKET': Variable.get("MINIO_BUCKET", default_var="raw"),
        'LOCAL_DIR': Variable.get("MINIO_LOCAL_DIR", default_var="/tmp/minio_downloads"),
        
        # Snowflake Config
        'SNOWFLAKE_USER': Variable.get("SNOWFLAKE_USER", default_var="Dali"),
        'SNOWFLAKE_PASSWORD': Variable.get("SNOWFLAKE_PASSWORD", default_var="Ihecdali11223344"),
        'SNOWFLAKE_ACCOUNT': Variable.get("SNOWFLAKE_ACCOUNT", default_var="rg77196.eu-west-3.aws"),
        'SNOWFLAKE_WAREHOUSE': Variable.get("SNOWFLAKE_WAREHOUSE", default_var="COMPUTE_WH"),
        'SNOWFLAKE_DB': Variable.get("SNOWFLAKE_DB", default_var="banking"),
        'SNOWFLAKE_SCHEMA': Variable.get("SNOWFLAKE_SCHEMA", default_var="raw"),
    }

TABLES = ["customers", "accounts", "transactions"]

# -------- Python Callables --------
def download_from_minio(**kwargs):
    """Download Parquet files from MinIO"""
    config = get_config()
    
    print("="*60)
    print("📥 DOWNLOADING FROM MINIO")
    print("="*60)
    print(f"Endpoint: {config['MINIO_ENDPOINT']}")
    print(f"Bucket: {config['MINIO_BUCKET']}")
    print(f"Local directory: {config['LOCAL_DIR']}")
    print("="*60)
    
    os.makedirs(config['LOCAL_DIR'], exist_ok=True)
    
    try:
        s3 = boto3.client(
            "s3",
            endpoint_url=config['MINIO_ENDPOINT'],
            aws_access_key_id=config['MINIO_ACCESS_KEY'],
            aws_secret_access_key=config['MINIO_SECRET_KEY']
        )
        
        local_files = {}
        total_files = 0
        
        for table in TABLES:
            prefix = f"{table}/"
            print(f"\n🔍 Looking for files in {config['MINIO_BUCKET']}/{prefix}...")
            
            try:
                resp = s3.list_objects_v2(Bucket=config['MINIO_BUCKET'], Prefix=prefix)
                objects = resp.get("Contents", [])
                
                if not objects:
                    print(f"   ⚠️  No files found for {table}")
                    local_files[table] = []
                    continue
                
                local_files[table] = []
                for obj in objects:
                    key = obj["Key"]
                    # Skip directory markers
                    if key.endswith('/'):
                        continue
                        
                    local_file = os.path.join(config['LOCAL_DIR'], os.path.basename(key))
                    s3.download_file(config['MINIO_BUCKET'], key, local_file)
                    local_files[table].append(local_file)
                    total_files += 1
                    print(f"   ✅ Downloaded: {key} -> {local_file}")
                    
            except Exception as e:
                print(f"   ❌ Error processing {table}: {e}")
                local_files[table] = []
        
        print("\n" + "="*60)
        print(f"✅ Download completed: {total_files} files total")
        print("="*60)
        
        return local_files
        
    except Exception as e:
        print(f"\n❌ Fatal error during MinIO download: {e}")
        raise

def load_to_snowflake(**kwargs):
    """Load data from local files to Snowflake"""
    config = get_config()
    
    print("="*60)
    print("❄️  LOADING TO SNOWFLAKE")
    print("="*60)
    print(f"Account: {config['SNOWFLAKE_ACCOUNT']}")
    print(f"Database: {config['SNOWFLAKE_DB']}")
    print(f"Schema: {config['SNOWFLAKE_SCHEMA']}")
    print(f"Warehouse: {config['SNOWFLAKE_WAREHOUSE']}")
    print("="*60)
    
    # Get files from previous task
    local_files = kwargs["ti"].xcom_pull(task_ids="download_minio")
    
    if not local_files:
        print("⚠️  No files found in MinIO. Nothing to load.")
        return
    
    # Count total files
    total_files = sum(len(files) for files in local_files.values())
    if total_files == 0:
        print("⚠️  No files to process. Exiting.")
        return
    
    print(f"\n📊 Total files to process: {total_files}")
    
    try:
        # Connect to Snowflake
        print("\n🔌 Connecting to Snowflake...")
        conn = snowflake.connector.connect(
            user=config['SNOWFLAKE_USER'],
            password=config['SNOWFLAKE_PASSWORD'],
            account=config['SNOWFLAKE_ACCOUNT'],
            warehouse=config['SNOWFLAKE_WAREHOUSE'],
            database=config['SNOWFLAKE_DB'],
            schema=config['SNOWFLAKE_SCHEMA'],
        )
        print("✅ Connected to Snowflake successfully!")
        
        cur = conn.cursor()
        
        # Process each table
        for table, files in local_files.items():
            if not files:
                print(f"\n⏭️  Skipping {table}: no files")
                continue
            
            print(f"\n{'='*60}")
            print(f"📋 Processing table: {table.upper()}")
            print(f"{'='*60}")
            print(f"Files to upload: {len(files)}")
            
            try:
                # Upload files to Snowflake internal stage
                for f in files:
                    print(f"   📤 Uploading: {f}")
                    put_sql = f"PUT file://{f} @%{table} AUTO_COMPRESS=FALSE OVERWRITE=TRUE"
                    cur.execute(put_sql)
                    print(f"   ✅ Uploaded to stage: @%{table}")
                
                # Copy data from stage into table
                print(f"\n   💾 Loading data into {table}...")
                copy_sql = f"""
                COPY INTO {table}
                FROM @%{table}
                FILE_FORMAT = (TYPE = 'PARQUET')
                MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
                ON_ERROR = 'CONTINUE'
                PURGE = TRUE
                """
                result = cur.execute(copy_sql)
                rows_loaded = result.fetchone()
                print(f"   ✅ Data loaded into {table}")
                if rows_loaded:
                    print(f"   📊 Rows loaded: {rows_loaded}")
                
            except snowflake.connector.errors.ProgrammingError as e:
                print(f"   ❌ Error loading {table}: {e}")
                # Continue with next table even if one fails
                continue
        
        cur.close()
        conn.close()
        
        print("\n" + "="*60)
        print("✅ SNOWFLAKE LOAD COMPLETED")
        print("="*60)
        
    except snowflake.connector.errors.DatabaseError as e:
        print(f"\n❌ Snowflake connection error: {e}")
        raise
    except Exception as e:
        print(f"\n❌ Unexpected error: {e}")
        raise

# -------- Airflow DAG Definition --------
default_args = {
    "owner": "airflow",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "email_on_failure": False,
    "email_on_retry": False,
}

with DAG(
    dag_id="minio_to_snowflake_banking",
    default_args=default_args,
    description="Load MinIO parquet files into Snowflake RAW tables",
    schedule_interval="*/5 * * * *",  # Every 5 minutes
    start_date=datetime(2025, 12, 1),
    catchup=False,
    tags=["banking", "minio", "snowflake", "etl"],
) as dag:

    download_task = PythonOperator(
        task_id="download_minio",
        python_callable=download_from_minio,
        provide_context=True,
    )

    load_task = PythonOperator(
        task_id="load_snowflake",
        python_callable=load_to_snowflake,
        provide_context=True,
    )

    download_task >> load_task