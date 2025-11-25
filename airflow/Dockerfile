FROM apache/airflow:2.7.3-python3.11

USER root

# Install Java (required for Spark)
RUN apt-get update && apt-get install -y \
    openjdk-11-jdk-headless \
    && rm -rf /var/lib/apt/lists/*

# Install Spark
ENV SPARK_VERSION=3.4.1
ENV SCALA_VERSION=2.12
ENV HADOOP_VERSION=3

RUN apt-get update && apt-get install -y wget && rm -rf /var/lib/apt/lists/*

RUN wget https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz && \
    tar -xzf spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz -C /opt && \
    ln -s /opt/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} /opt/spark && \
    rm spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz

ENV SPARK_HOME=/opt/spark
ENV PATH=$PATH:$SPARK_HOME/bin

USER airflow

# Install Python dependencies including Airflow Spark provider
RUN pip install --no-cache-dir \
    apache-airflow-providers-apache-spark==4.4.0 \
    confluent-kafka \
    pyspark==${SPARK_VERSION}
