FROM apache/airflow:2.7.3-python3.8
USER root
RUN apt-get update && apt-get install -y openjdk-11-jre && rm -rf /var/lib/apt/lists/*
USER airflow
