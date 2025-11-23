FROM apache/airflow:2.8.1

USER root

# Installer dépendances système pour confluent_kafka et psycopg2
RUN apt-get update && \
    apt-get install -y gcc g++ librdkafka-dev && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

# Repasser à l'utilisateur airflow pour installer pip packages
USER airflow

# Installer packages Python nécessaires
RUN pip install --no-cache-dir confluent_kafka requests psycopg2-binary
