from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.decorators.task_group import task_group
from airflow.models import Variable

from pathlib import Path
import sys, os

sys.path.append('../../')  # Replace with the actual path to the data_ingestion directory

execution_path = Variable.get("execution_path")

# Default parameters for the workflow
default_args = {
    'depends_on_past': False,
    'owner': 'airflow',
    'start_date': datetime(2022, 11, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

@task_group(group_id='Data_Ingestion')
def tg1():
    data_ingestion_1 = BashOperator(
        task_id='data_ingestion_1',
        bash_command=f"python3 {execution_path}/data_ingestion/data_ingestion.py --api_endpoint <you_endpoint> --temporal_landing_zone_path <your_path> --filename <filename>"
    )

    data_ingestion_1

with DAG(
        'Data_Management_Backbone', # Name of the DAG / workflow
        default_args=default_args,
        catchup=False,
        schedule='*/15 * * * *' 
) as dag:
    # This operator does nothing. 
    start_task = EmptyOperator(
        task_id='set_envs', 
    )

    persistent_landing_zone = BashOperator(
        task_id='persistent_landing_zone',
        bash_command=f"python3 {execution_path}/persistent_landing_zone/load_data_to_gcs.py --bucket_name <bucket_name> --folder_path {execution_path}/temporal_landing_zone --destination_folder persistent"
    )

    formatted_zone = BashOperator(
        task_id='formatted_zone',
        bash_command=f"python3 {execution_path}/formatted_zone/move_data_to_formatted_zone.py --bucket <bucket_name>"
    )

    trusted_zone = BashOperator(
        task_id='trusted_zone',
        bash_command=f"python3 {execution_path}/trusted_zone/trusted_zone.py"
    )

    exploitation_zone = BashOperator(
        task_id='exploitation_zone',
        bash_command=f"python3 {execution_path}/exploitation_zone/exploitation_zone.py"
    )    

    dbt_exploitation_zone = BashOperator(
        task_id='dbt_exploitation_zone',
        env={'ez_host': '{{var.value.dbhost}}', 'ez_user': '{{var.value.dbuser}}', 'explotation_zone_creds': '{{var.value.formatted_zone_secret}}', 'ez_dbname': '{{var.value.ez_dbname}}'},
        bash_command=f"cd {execution_path}/dbt/exploitation_zone && {Variable.get('dbt_instalation_path')} run -t prod --profiles-dir {execution_path}/dbt/"
    )   


    print(Variable.get('dbhost'))
    # Define the order in which the tasks are supposed to run
    # You can also define paralell tasks by using an array 
    # I.e. task1 >> [task2a, task2b] >> task3
    start_task >> tg1() >> persistent_landing_zone >> formatted_zone >> trusted_zone >> exploitation_zone >> dbt_exploitation_zone

