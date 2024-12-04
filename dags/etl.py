from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

default_args = {
  'owner': 'duc',
  'depends_on_past': False,
  'start_date': datetime(2023,3,20),
  'retries': 0
}

with DAG('etl' , default_args= default_args, schedule= '@once') as dag : 
    
    
    etl = BashOperator(
        task_id = "etl",
        bash_command = "python WebCrawler.py",
        cwd= './Real-Estate-Analysis'
    )
    
    end = EmptyOperator(
        task_id= 'done',
        trigger_rule= 'all_success'
    )
    
    etl  >> end