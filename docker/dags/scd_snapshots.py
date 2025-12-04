from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

DBT_PATH = "/home/airflow/.local/bin/dbt"

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="banking_dbt_pipeline",
    default_args=default_args,
    description="Complete dbt pipeline",
    schedule_interval="@daily",
    start_date=datetime(2025, 12, 1),
    catchup=False,
    tags=["dbt", "banking"],
) as dag:

    dbt_debug = BashOperator(
        task_id="dbt_debug",
        bash_command=f"""
        cd /opt/airflow/banking_dbt &&
        {DBT_PATH} debug --profiles-dir /home/airflow/.dbt
        """
    )

    dbt_run_staging = BashOperator(
        task_id="dbt_run_staging",
        bash_command=f"""
        cd /opt/airflow/banking_dbt &&
        {DBT_PATH} run --select staging --profiles-dir /home/airflow/.dbt
        """
    )

    # 🔥 CORRECT → exécute TOUS les snapshots
    dbt_snapshot = BashOperator(
        task_id="dbt_snapshot",
        bash_command=f"""
        cd /opt/airflow/banking_dbt &&
        {DBT_PATH} snapshot --profiles-dir /home/airflow/.dbt
        """
    )

    dbt_run_dimensions = BashOperator(
        task_id="dbt_run_dimensions",
        bash_command=f"""
        cd /opt/airflow/banking_dbt &&
        {DBT_PATH} run --select marts.dimensions --profiles-dir /home/airflow/.dbt
        """
    )

    dbt_run_facts = BashOperator(
        task_id="dbt_run_facts",
        bash_command=f"""
        cd /opt/airflow/banking_dbt &&
        {DBT_PATH} run --select marts.facts --profiles-dir /home/airflow/.dbt
        """
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"""
        cd /opt/airflow/banking_dbt &&
        {DBT_PATH} test --profiles-dir /home/airflow/.dbt
        """
    )

    dbt_docs_generate = BashOperator(
        task_id="dbt_docs_generate",
        bash_command=f"""
        cd /opt/airflow/banking_dbt &&
        {DBT_PATH} docs generate --profiles-dir /home/airflow/.dbt
        """
    )

    # Dependencies
    dbt_debug >> dbt_run_staging >> dbt_snapshot >> dbt_run_dimensions >> dbt_run_facts >> dbt_test >> dbt_docs_generate
