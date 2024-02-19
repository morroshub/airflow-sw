from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.sensors.sql_sensor import SqlSensor
from airflow.utils.task_group import TaskGroup

from datetime import timedelta, datetime
import pandas as pd
from ydata_profiling import ProfileReport
import pytz


# Activamos la imagen de docker para interactuar con un CLI de airflow
 # docker exec -it 'ID-Contenedor' bash
 # docker exec -it f7b806ca4b36 bash

# Consumo de datos
URL = 'https://raw.githubusercontent.com/ronnygang/webinar/main/titanic.csv'
PATH = '/opt/airflow/dags/data/titanic.csv'

DEFAULT_ARGS = {
    'owner': 'Morros',
    'retries' : 2,
    'retry_delay': timedelta(minutes=0.5)
}


with DAG  (
    'dag_principal',
    description = 'Ejecucion de pipeline principal',
    default_args =  DEFAULT_ARGS,
    catchup = False,
    start_date = datetime(2024,2,19),
    schedule_interval = '@once',
    tags = ['principal']

) as dag:
    descargar = BashOperator(
        task_id = 'descargar_csv',
        bash_command = 'curl -o {{ params.path }} {{ params.url}}',
        params = {
            'path' : PATH,
            'url' : URL
        }
    )

# Test dag para ahorrar recursos.
    #airflow task test dag + task 
        # airflow tasks test dag_principal descargar_csv 2024-19-02
