import os


# Import Airflow
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.sensors.sql_sensor import SqlSensor
from airflow.utils.task_group import TaskGroup


# Imports time
from datetime import timedelta, datetime
import pytz

# Import pandas
import pandas as pd
from ydata_profiling import ProfileReport
import matplotlib
matplotlib.use('Agg')  # Use Agg backend (non-interactive) for saving plots



# Activamos la imagen de docker para interactuar con un CLI de airflow
 # docker exec -it 'ID-Contenedor' bash
 # docker exec -it f7b806ca4b36 bash

# Set date time and structure 
TZ = pytz.timezone('America/Argentina/Buenos_Aires')
TODAY = datetime.now(TZ).strftime('%Y-%m-%d')

# Consumo de datos
URL = 'https://raw.githubusercontent.com/ronnygang/webinar/main/titanic.csv'
PATH = '/opt/airflow/dags/data/titanic.csv'
OUTPUT_DQ = '/opt/airflow/dags/data/data_quality_report_{}.html'.format(TODAY)
OUTPUT_SQL = '/opt/airflow/dags/sql/titanic_{}.sql'.format(TODAY)
OUTPUT = '/opt/airflow/dags/data/titanic_curated_{}.csv'.format(TODAY)


DEFAULT_ARGS = {
    'owner': 'Airflow',
    'retries' : 2,
    'retry_delay': timedelta(minutes=0.5)
}

# Funcion para crear el archivo mejorado html del CSV

def _profilling():
        # Configura MPLCONFIGDIR antes de utilizar pandas_profiling
    os.environ['MPLCONFIGDIR'] = '/opt/airflow/dags/matplotlib_cache'

    df = pd.read_csv(PATH)
    profile = ProfileReport(df, title= 'Data Quality Report')

    # Ruta de salida
    profile.to_file(OUTPUT_DQ)


def _curated():
    # Lectura de archivo csv
    df = pd.read_csv(PATH)

    # Eliminamos columnas no necesarias
    df.drop(['Ticket', 'Cabin'], axis=1, inplace=True)

    # Rellenamos valores faltantes
    df['Age'].fillna(df['Age'].median(), inplace=True)
    df['Embarked'].fillna(df['Embarked'].mode()[0], inplace=True)

    # Modificar la columna 'Name para obtener el nombre y apelido
    df['Full Name'] = df['Name'].apply(lambda x: ''.join(x.split(',')[1].split('(')[0].strip().split()[1:]))

    df['Full Name'] = df['Full Name'].str.replace('["\']', '', regex=True)

    # Agregar una nueva columna 'Title' a partir de la columna 'Name'
    df['Title'] = df ['Name'].apply(lambda x: x.split(',')[1].split('.')[0].strip())

    # Elimno la columna 'Name'
    df.drop(['Name'], axis=1, inplace=True)

    # Simplificar los titulos
    title_dict = {
        'Capt' : 'Officer',
        'Col' : 'Officer',
        'Major' : 'Officer',
        'Dr' : 'Officer',
        'Rev' : 'Officer',
        'Jonkheer' : 'Royalty',
        'Don' : 'Royalty',
        'Sir' : 'Royalty',
        'the Countess' : 'Royalty',
        'Lady' : 'Royalty',
        'Ms' : 'Mrs',
        'Mme' : 'Mrs',
        'Mr' : 'Mr',
        'Mlle' : 'Miss',
        'Master' : 'Master',

    }

    # Mapeo de titulos
    df['Title'] = df['Title'].map(title_dict)

    # Reordenar las columnas en el orden deseado
    df = df[['PassengerId', 'Full Name', 'Title', 'Survived', 'Pclass', 'Sex', 'Age', 'SibSp', 'Parch', 'Fare', 'Embarked']]

    #Guardar el archivo procesado con todos los cambios en formato Csv
    df.to_csv(OUTPUT, index=False)

    # Iterar sobre las filas y crear los inserts
    with open(OUTPUT_SQL, 'w') as f:
        for index, row in df.iterrows():
            values = f"({row['PassengerId']}, {row['Full Name']}, '{row['Title']}, {row['Survived']}, {row['Pclass']}, {row['Sex']}, {row['Age']}, {row['SibSp']}, {row['Parch']}, {row['Fare']},{row['Embarked']})" 
            insert = f"INSERT INTO raw_titanic VALUES {values};\n"
            f.write(insert)

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
        # airflow tasks test dag_principal descargar_csv 2024-02-19



# Profiling del archivo descargado 
    
profiling = PythonOperator(
    dag=dag,
    task_id = 'profiling',
    python_callable = _profilling,
)

# Test
# airflow tasks test dag_principal profiling 2024-02-19

curated = PythonOperator(
    dag=dag,
    task_id = 'curated',
    python_callable = _curated,
)

# Test
# airflow tasks test dag_principal curated 2024-02-19