from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timezone
import requests
from pymongo import MongoClient

# 1. EXTRACT TASK
def extract_rates(**kwargs):
    url = "https://api.frankfurter.dev/v2/rates?base=EUR&quotes=USD,GBP,JPY"
    response = requests.get(url)
    
    # Fallback to v1 endpoint if v2 returns 404 or a list format
    if response.status_code != 200:
        url = "https://api.frankfurter.dev/v1/latest?base=EUR&symbols=USD,GBP,JPY"
        response = requests.get(url)
        
    raw_data = response.json()
    
    # Push to Named XCom
    ti = kwargs['ti']
    ti.xcom_push(key='raw_exchange_rates', value=raw_data)
    print("Extracted Raw Data:", raw_data)


# 2. TRANSFORM TASK
def transform_rates(**kwargs):
    ti = kwargs['ti']
    # Pull from Named XCom
    raw_data = ti.xcom_pull(task_ids='extract_exchange_rates', key='raw_exchange_rates')
    
    retrieved_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")
    transformed_records = []

    # Handle both v2 list format and v1 dict format from Frankfurter API
    if isinstance(raw_data, list):
        for item in raw_data:
            transformed_records.append({
                "base_currency": item.get("base", "EUR"),
                "target_currency": item.get("quote"),
                "exchange_rate": float(item.get("rate")),
                "rate_date": item.get("date"),
                "source": "Frankfurter API",
                "retrieved_at": retrieved_at
            })
    elif isinstance(raw_data, dict):
        base = raw_data.get("base", "EUR")
        rate_date = raw_data.get("date")
        rates = raw_data.get("rates", {})
        for target_curr, rate_val in rates.items():
            transformed_records.append({
                "base_currency": base,
                "target_currency": target_curr,
                "exchange_rate": float(rate_val),
                "rate_date": rate_date,
                "source": "Frankfurter API",
                "retrieved_at": retrieved_at
            })

    # Push transformed data to Named XCom
    ti.xcom_push(key='transformed_exchange_rates', value=transformed_records)
    print("Transformed Data:", transformed_records)


# 3. LOAD TASK
def load_rates_to_mongo(**kwargs):
    ti = kwargs['ti']
    # Pull transformed data from Named XCom
    records = ti.xcom_pull(task_ids='transform_exchange_rates', key='transformed_exchange_rates')
    
    # Connect to MongoDB container using Docker service name 'mongodb'
    client = MongoClient("mongodb://admin:password123@mongodb:27017/")
    db = client["currency_db"]
    collection = db["exchange_rates"]
    
    if records:
        collection.insert_many(records)
        print(f"Successfully inserted {len(records)} records into MongoDB!")
    else:
        print("No records found to insert.")


# DEFINE DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2026, 11, 1),
    'retries': 1,
}

with DAG(
    dag_id='frankfurter_currency_etl',
    default_args=default_args,
    schedule='@daily',
    catchup=False,
    description='Daily ETL pipeline for Frankfurter currency rates to MongoDB'
) as dag:

    extract_task = PythonOperator(
        task_id='extract_exchange_rates',
        python_callable=extract_rates,
    )

    transform_task = PythonOperator(
        task_id='transform_exchange_rates',
        python_callable=transform_rates,
    )

    load_task = PythonOperator(
        task_id='load_data_to_mongodb',
        python_callable=load_rates_to_mongo,
    )

    # Set task dependencies
    extract_task >> transform_task >> load_task