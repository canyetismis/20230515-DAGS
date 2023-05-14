from airflow import DAG
from airflow.operators.python import PythonOperator

from datetime import datetime

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 4, 10),
    'retries': 0
}

dag = DAG(
    dag_id='0_example_dag',
    default_args=default_args,
    schedule_interval=None,
    max_active_runs=1
)

def task(x: int):
    print(x)

task_1 = PythonOperator(
    task_id='task_1',
    python_callable=task,
    op_kwargs={
        'x': 1
    },
    dag=dag,
)

task_2 = PythonOperator(
    task_id='task_2',
    python_callable=task,
    op_kwargs={
        'x': 1
    },
    dag=dag,
)

task_3 = PythonOperator(
    task_id='task_3',
    python_callable=task,
    op_kwargs={
        'x': 1
    },
    dag=dag,
)

task_1 >> [task_2, task_3]