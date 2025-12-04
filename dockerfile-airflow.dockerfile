# Dockerfile-airflow
FROM apache/airflow:2.9.3

# Switch to root to install system packages
USER root

# Install git + system deps
RUN apt-get update && apt-get install -y --no-install-recommends \
    git \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Switch back to airflow user
USER airflow

# Install dbt (core + snowflake)
RUN pip install --no-cache-dir dbt-core dbt-snowflake
