FROM apache/spark-py:latest

USER root

# System packages needed for connectivity and health checks.
RUN apt-get update && apt-get install -y --no-install-recommends curl unzip && \
    rm -rf /var/lib/apt/lists/*

# Python dependencies for ETL workloads and object storage integration.
RUN python3 -m pip install --no-cache-dir \
    boto3 \
    pyarrow \
    pandas \
    python-dotenv

# Runtime jars: Oracle JDBC, Hadoop AWS and AWS SDK for S3A on MinIO.
RUN mkdir -p /opt/spark/jars-extra && \
    curl -fsSL -o /opt/spark/jars-extra/ojdbc11.jar https://repo1.maven.org/maven2/com/oracle/database/jdbc/ojdbc11/23.4.0.24.05/ojdbc11-23.4.0.24.05.jar && \
    curl -fsSL -o /opt/spark/jars-extra/hadoop-aws-3.3.4.jar https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar && \
    curl -fsSL -o /opt/spark/jars-extra/aws-java-sdk-bundle-1.12.262.jar https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar

RUN cp /opt/spark/jars-extra/*.jar /opt/spark/jars/

ENV SPARK_HOME=/opt/spark
ENV SPARK_EXTRA_CLASSPATH=/opt/spark/jars/ojdbc11.jar
