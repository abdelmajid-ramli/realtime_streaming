from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
import requests
import json
from confluent_kafka import Producer

# ------------------------------------------
# 1. FONCTION FETCH ET STOCKAGE POSTGRES
# ------------------------------------------
def fetch_info_only(**context):
    """
    Appelle l'API randomuser.me,
    extrait uniquement 'info',
    stocke dans PostgreSQL,
    et PUSH 'results' dans XCom pour Kafka.
    """
    # 1. Appel API
    url = "https://randomuser.me/api/?results=10"
    response = requests.get(url)
    response.raise_for_status()

    data = response.json()
    info = data.get("info", {})
    results = data.get("results", [])   # ← important pour Kafka

    # 2. Extraction
    seed = info.get("seed")
    results_count = info.get("results")
    page = info.get("page")
    version = info.get("version")

    # 3. Save to PostgreSQL
    hook = PostgresHook(postgres_conn_id="postgres_randomuser")
    conn = hook.get_conn()

    with conn.cursor() as cursor:
        cursor.execute("""
            INSERT INTO randomuser_info (seed, results, page, version, raw_json)
            VALUES (%s, %s, %s, %s, %s)
        """, (seed, results_count, page, version, json.dumps(info)))
    conn.commit()

    # 4. Push results dans XCom pour Kafka
    context["ti"].xcom_push(key="randomuser_results", value=results)


# ------------------------------------------
# 2. FONCTION ENVOI KAFKA
# ------------------------------------------
def send_results_to_kafka(**context):
    """
    Récupère results depuis XCom et les envoie vers Kafka.
    """
    results = context["ti"].xcom_pull(
        key="randomuser_results",
        task_ids="fetch_and_store_info"
    )

    if not results:
        raise ValueError("Aucun résultat récupéré pour Kafka.")

    # Configuration Kafka directement
    producer = Producer({'bootstrap.servers': 'kafka_new:9092'})
    topic = "randomuser_results"

    for user in results:
        producer.produce(topic=topic, value=json.dumps(user).encode('utf-8'))

    producer.flush()


# ------------------------------------------
# 3. DAG AIRFLOW
# ------------------------------------------
default_args = {
    "start_date": datetime(2024, 1, 1),
}

with DAG(
    dag_id="randomuser_info_and_kafka",
    default_args=default_args,
    schedule_interval="@hourly",
    catchup=False,
    description="Stocke 'info' dans PostgreSQL + envoie 'results' vers Kafka."
) as dag:

    # Tâche 1 : PostgreSQL
    fetch_task = PythonOperator(
        task_id="fetch_and_store_info",
        python_callable=fetch_info_only,
        provide_context=True
    )

    # Tâche 2 : Kafka
    kafka_task = PythonOperator(
        task_id="send_results_to_kafka",
        python_callable=send_results_to_kafka,
        provide_context=True
    )

    # Dépendances
    fetch_task >> kafka_task
