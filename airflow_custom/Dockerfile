FROM apache/airflow:3.1.0-python3.10
USER root
RUN apt-get update && apt-get install -y default-jdk procps && rm -rf /var/lib/apt/lists/*
# Install Spark
ENV SPARK_VERSION=3.5.7
RUN wget -q https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop3.tgz && \
    tar xzf spark-${SPARK_VERSION}-bin-hadoop3.tgz -C /opt && \
    rm spark-${SPARK_VERSION}-bin-hadoop3.tgz && \
    ln -s /opt/spark-${SPARK_VERSION}-bin-hadoop3 /opt/spark

ENV SPARK_HOME=/opt/spark
ENV PATH=$PATH:$SPARK_HOME/bin

RUN echo "JAVA_HOME=/usr/lib/jvm/java-17-openjdk-arm64" >> /etc/environment
USER airflow
RUN pip install apache-airflow apache-airflow-providers-apache-spark kafka-python