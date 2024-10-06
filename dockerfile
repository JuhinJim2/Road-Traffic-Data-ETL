FROM apache/airflow:latest

USER root
RUN apt-get update && \
    apt-gte -y install git && \
    apt-get clean
USER airflow