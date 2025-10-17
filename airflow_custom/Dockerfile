FROM apache/airflow:latest
USER root
RUN apt-get update && apt-get install -y default-jdk procps && rm -rf /var/lib/apt/lists/*
RUN echo "JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64" >> /etc/environment
USER airflow