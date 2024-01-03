# importing the libraries 

from datetime import timedelta
# DAG object; this is needed to instantiate a DAG
from airflow import DAG
#Operator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
# making scheduling easy
from airflow.utils.dates import days_ago
# Importing other libraries for downloading and saving file
import os, tarfile
import pandas as pd

CUR_DIR = os.path.abspath(os.path.dirname(__file__))

# Defining DAG Arguments
default_args = {
    'owner': 'Karthik J Ghorpade',
    'start_date': days_ago(0),
    'email': ['karthik@xyz.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delta': timedelta(minutes=5)
}

# Defining the DAG
dag = DAG(
    'ETL_toll_data',
    default_args=default_args,
    description='Apache Airflow Final Assignment',
    schedule_interval=timedelta(days=1),
    )

#First Task - Unzip Data

def unzip_file(file, target_path):
    if not os.path.exists(file):
        raise FileNotFoundError(f"The file {file} does not exist.")
    with tarfile.open(file, "r:gz") as tar:
        tar.extractall(target_path)

unzip_data = PythonOperator(
    task_id='unzip_data',
    python_callable = unzip_file,
    op_kwargs = {
        "file" : f"{CUR_DIR}/finalassignment/tolldata.tgz",
        "target_path" : f"{CUR_DIR}/finalassignment/staging"
    },
    dag=dag
)

# Second Task - Extract Data from CSV

def extract_data_from_csv(file, target_path, columns_to_extract):
    if not os.path.exists(file):
        raise FileNotFoundError(f"The file {file} does not exist.")
    df = pd.read_csv(file)
    new_df = df.iloc[:, columns_to_extract]
    new_df.to_csv(target_path, index = False)

extract_data_from_csv = PythonOperator(
    task_id='extract_data_from_csv',
    python_callable = extract_data_from_csv,
    op_kwargs = {
        "file" : f"{CUR_DIR}/finalassignment/staging/vehicle-data.csv",
        "target_path" : f"{CUR_DIR}/finalassignment/staging/csv_data.csv",
        "columns_to_extract" : [0,1,2,3]
    },
    dag=dag
)

# Third Task - Extract Data from TSV

def extract_data_from_tsv(file, target_path, columns_to_extract):
    if not os.path.exists(file):
        raise FileNotFoundError(f"The file {file} does not exist.")
    df = pd.read_csv(file, sep='\t')
    new_df = df.iloc[:, columns_to_extract]
    new_df.to_csv(target_path, index = False)

extract_data_from_tsv = PythonOperator(
    task_id='extract_data_from_tsv',
    python_callable = extract_data_from_tsv,
    op_kwargs = {
        "file" : f"{CUR_DIR}/finalassignment/staging/tollplaza-data.tsv",
        "target_path" : f"{CUR_DIR}/finalassignment/staging/tsv_data.csv",
        "columns_to_extract" : [4,5,6]
    },
    dag=dag
)

# Fourth Task - Extract Data from Fixed Width File

def extract_data_from_fixed_width(file, target_path, columns_to_extract):
    if not os.path.exists(file):
        raise FileNotFoundError(f"The file {file} does not exist.")
    df = pd.read_fwf(file)
    new_df = df.iloc[:, columns_to_extract]
    new_df.to_csv(target_path, index = False)

extract_data_from_fixed_width = PythonOperator(
    task_id='extract_data_from_fixed_width',
    python_callable = extract_data_from_fixed_width,
    op_kwargs = {
        "file" : f"{CUR_DIR}/finalassignment/staging/payment-data.txt",
        "target_path" : f"{CUR_DIR}/finalassignment/staging/fixed_width_data.csv",
        "columns_to_extract" : [9,10]
    },
    dag=dag
)

# Fifth Task - Consolidate Data

bash_command='''paste -d "," /home/project/airflow/dags/finalassignment/staging/csv_data.csv \
                /home/project/airflow/dags/finalassignment/staging/tsv_data.csv \
                /home/project/airflow/dags/finalassignment/staging/fixed_width_data.csv \
                > /home/project/airflow/dags/finalassignment/staging/extracted_data.csv'''

consolidate_data = BashOperator(
    task_id='consolidate_data',
    bash_command=bash_command,
    dag=dag
)

# Sixth Task - Transform Data

def capitalize(file, target_path, column_to_capitalize):
    if not os.path.exists(file):
        raise FileNotFoundError(f"The file {file} does not exist.")
    df = pd.read_csv(file, header=None)
    new_df = df.copy()
    new_df[column_to_capitalize] = new_df[column_to_capitalize].apply(lambda x: x.upper())
    new_df.to_csv(target_path, index = False, header = None)

transform_data = PythonOperator(
    task_id='transform_data',
    python_callable = capitalize,
    op_kwargs = {
        "file" : "/home/project/airflow/dags/finalassignment/staging/extracted_data.csv",
        "target_path" : "/home/project/airflow/dags/finalassignment/staging/transformed_data.csv",
        "column_to_capitalize" : 3
    },
    dag=dag
)


unzip_data >> extract_data_from_csv >> extract_data_from_tsv >> extract_data_from_fixed_width  >> consolidate_data >> transform_data