from datetime import datetime
from datetime import timedelta

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash_operator import BashOperator

from airflow.utils.dates import days_ago


default_args = {
    'owner': 'airflow'}

dag = DAG(
    dag_id='bigdata_processings',
    start_date=days_ago(1),
    schedule_interval='*/2 * * * *',
    default_args=default_args)

task1 = DummyOperator(
    task_id='job_start')

task2 = BashOperator(
    task_id='dump_data',
    bash_command='python3 /opt/airflow/dags/script/dump.py',
    dag=dag)

task3 = BashOperator(
    task_id='etl_mapreduce',
    bash_command='python3 /opt/airflow/dags/script/mapreduce_etl.py',
    dag=dag)

task4 = DummyOperator(
    task_id='job_finish')

    # orchestrator
(
    task1
    >> task2
    >> task3
    >> task4
)