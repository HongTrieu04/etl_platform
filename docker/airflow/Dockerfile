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
    pyspark==3.5.1 \
    "apache-airflow-providers-apache-spark>=4.0,<5"

USER root
RUN mkdir -p /opt/airflow/jars && \
    curl -fsSL -o /opt/airflow/jars/ojdbc8.jar https://repo1.maven.org/maven2/com/oracle/database/jdbc/ojdbc8/19.24.0.0/ojdbc8-19.24.0.0.jar && \
    curl -fsSL -o /opt/airflow/jars/hadoop-aws-3.3.4.jar https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar && \
    curl -fsSL -o /opt/airflow/jars/aws-java-sdk-bundle-1.12.262.jar https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar
USER airflow
