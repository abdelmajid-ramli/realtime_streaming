from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
import requests
import json

def fetch_info_only():
    """
    Appelle l'API randomuser.me,
    extrait uniquement la partie 'info',
    et stocke les métadonnées dans PostgreSQL.
    """
    # 1. Appel API
    url = "https://randomuser.me/api/?results=10"
    response = requests.get(url)
    response.raise_for_status()
    info = response.json().get("info", {})

    # 2. Extraction des champs
    seed = info.get("seed")
    results = info.get("results")
    page = info.get("page")
    version = info.get("version")

    # 3. Connexion à PostgreSQL via Airflow
    hook = PostgresHook(postgres_conn_id="postgres_randomuser")
    conn = hook.get_conn()

    with conn.cursor() as cursor:
        # 4. Insertion SQL
        cursor.execute("""
            INSERT INTO randomuser_info (seed, results, page, version, raw_json)
            VALUES (%s, %s, %s, %s, %s)
        """, (seed, results, page, version, json.dumps(info)))

    conn.commit()


# 5. Paramètres du DAG
default_args = {
    "start_date": datetime(2024, 1, 1),
}

# 6. Déclaration du DAG
with DAG(
    dag_id="randomuser_info_only",
    default_args=default_args,
    schedule_interval="@hourly",  # modifier si besoin
    catchup=False,
    description="Stocker uniquement la partie 'info' de randomuser.me dans PostgreSQL"
) as dag:

    fetch_task = PythonOperator(
        task_id="fetch_and_store_info",
        python_callable=fetch_info_only
    )
