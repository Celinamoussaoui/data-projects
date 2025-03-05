from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), "../scripts"))
from etl_functions import create_table, extract, transform, load

#Définition du DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}
dag = DAG(
    'etl_pipeline',
    default_args=default_args,
    schedule=timedelta(days=1),
    catchup=False,
)
#Tâche de création de tables
task_create_table = PythonOperator(
    task_id='create_table',
    python_callable=create_table,
    dag=dag,
)
#Tâche d'extraction
task_extract = PythonOperator(
    task_id='extract',
    python_callable=extract,
    dag=dag,
)
#Tâche de transformation
task_transform = PythonOperator(
    task_id='transform',
    python_callable=transform,
    dag=dag,
)
#Tâche de chargement
task_load = PythonOperator(
    task_id='load',
    python_callable=load,
    dag=dag,
)

task_create_table >> task_extract >> task_transform >> task_load