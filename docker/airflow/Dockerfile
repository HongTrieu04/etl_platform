FROM apache/airflow:2.10.3-python3.11

USER root
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    build-essential \
    default-jre-headless \
    && rm -rf /var/lib/apt/lists/*
USER airflow

# Lightweight ETL packages used by the current skeleton stack.
RUN pip install --no-cache-dir \
    oracledb==2.5.1 \
    boto3 \
    pandas \
    pyarrow \
    apache-airflow-providers-apache-spark>=4.0,<5
