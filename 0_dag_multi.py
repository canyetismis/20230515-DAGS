import os
import random
import time
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from logger import log_line

#seed = 111
type = "parallel"
log_name = "dag_final_multi_sequential"
LOG_FILE = os.getcwd() + "/dags/log/"+log_name+".log"

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 4, 10),
    'retries': 0
}

dag = DAG(
    dag_id='0_dag_multi',
    default_args=default_args,
    schedule_interval=None,
    max_active_runs=1
)

###################################
##### Configuration Variables #####
###################################


seconds = 1
throughput_measure_intervals = [
    (15 * seconds),
    (30 * seconds),
    (45 * seconds),
    ((1 * 60) * seconds),
    (((1 * 60) * seconds) + (15 * seconds)),
    (((1 * 60) * seconds) + (30 * seconds)),
    (((1 * 60) * seconds) + (45 * seconds)),
    ((2 * 60) * seconds),
    (((2 * 60) * seconds) + (15 * seconds)),
    (((2 * 60) * seconds) + (30 * seconds)),
    (((2 * 60) * seconds) + (45 * seconds)),
    ((3 * 60) * seconds),
    (((3 * 60) * seconds) + (15 * seconds)),
    (((3 * 60) * seconds) + (30 * seconds)),
    (((3 * 60) * seconds) + (45 * seconds)),
    ((4 * 60) * seconds),
    (((4 * 60) * seconds) + (15 * seconds)),
    (((4 * 60) * seconds) + (30 * seconds)),
    (((4 * 60) * seconds) + (45 * seconds)),
    ((5 * 60) * seconds),
    (((5 * 60) * seconds) + (15 * seconds)),
    (((5 * 60) * seconds) + (30 * seconds)),
    (((5 * 60) * seconds) + (45 * seconds)),
    ((6 * 60) * seconds),
    (((6 * 60) * seconds) + (15 * seconds)),
    (((6 * 60) * seconds) + (30 * seconds)),
    (((6 * 60) * seconds) + (45 * seconds)),
    ((7 * 60) * seconds),
    (((7 * 60) * seconds) + (15 * seconds)),
    (((7 * 60) * seconds) + (30 * seconds)),
    (((7 * 60) * seconds) + (45 * seconds)),
    ((8 * 60) * seconds),
]

def get_extract_transform_params():
    #random.seed(seed)
    runtime = random.randint(10, 15) * seconds
    input = random.randint(800, 1200)
    return [runtime, input]

def get_processor_params():
    #random.seed(seed)
    runtime = random.randint(25, 30) * seconds
    input = random.randint(600, 750)
    return [runtime, input]

def get_classifier_params():
    #random.seed(seed)
    runtime = random.randint(40, 45) * seconds
    input = random.randint(400, 600)
    return [runtime, input]

###########################
##### Mimic Functions #####
###########################

def set_start(**context):
    #random.seed(111)
    dag_start = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    context['ti'].xcom_push(key='dag_start', value=dag_start)

def mimic(name, kernel, **context):
    runtime, input = kernel()
    time.sleep(runtime)
    
    dag_start = datetime.strptime(context['ti'].xcom_pull(task_ids='start_task', key='dag_start'), "%Y-%m-%d %H:%M:%S")
    end = datetime.now()
    for index, value in enumerate(throughput_measure_intervals):
        match_flag = False
        if index == 0:
            if (end  - dag_start) <= timedelta(seconds=value):
                context['ti'].xcom_push(key=f'{name}_throughput', value=input)
                context['ti'].xcom_push(key=f'{name}_record_time', value=value)
                match_flag = True
        else:
            if (end  - dag_start) > timedelta(seconds=throughput_measure_intervals[index-1]) and (end  - dag_start) <= timedelta(seconds=value):
                context['ti'].xcom_push(key=f'{name}_throughput', value=input)
                context['ti'].xcom_push(key=f'{name}_record_time', value=value)
                match_flag = True
        if match_flag:
            break

def print_total_throughput(**context):
    start = datetime.now()
    dag_start = datetime.strptime(context['ti'].xcom_pull(task_ids='start_task', key='dag_start'), "%Y-%m-%d %H:%M:%S")
    delta = start - dag_start
    log_line(LOG_FILE, "----- START LOG -----")
    log_line(LOG_FILE, "Total Runtime: " + str(delta))
    
    task_list = []
    for task in ['extract_transform_1', 'extract_transform_2', 'extract_transform_3', 'processor_1', 'processor_2', 'processor_3', 'processor_4', 'processor_5', 'processor_6', 'classifier_1', 'classifier_2', 'classifier_3', 'classifier_4']:
        throughput = context['ti'].xcom_pull(task_ids=task, key=f'{task}_throughput')
        record_time = context['ti'].xcom_pull(task_ids=task, key=f'{task}_record_time')
        
        if throughput and record_time:
            #total_throughput += throughput 
            
            if "extract" in task: 
                task_list.append(
                    {
                        'task' : task,
                        'level': 1,
                        'record_time': record_time,
                        'throughput': throughput
                    }
                )
            
            if "processor" in task: 
                task_list.append(
                    {
                        'task' : task,
                        'level': 2,
                        'record_time': record_time,
                        'throughput': throughput
                    }
                )
            
            if "classifier" in task: 
                task_list.append(
                    {
                        'task' : task,
                        'level': 3,
                        'record_time': record_time,
                        'throughput': throughput
                    }
                )
            #log_line(LOG_FILE, "Throughput: " + str(total_throughput) + " record time: " + str(record_time) +" task: " + task + " throughput: " + str(throughput))
    def sort_key(item):
        return (item["level"], item["record_time"])
    
    task_list = sorted(task_list, key=sort_key)
    log_line(LOG_FILE, str(task_list))
    total_throughput = 0
    for task in task_list:
        total_throughput += task['throughput']
        log_line(LOG_FILE, "Throughput: " + str(total_throughput) + " record time: " + str(task['record_time']) +" task: " + str(task['task']) + " throughput: " + str(task['throughput']))
    
    log_line(LOG_FILE, "Total throughput: " + str(total_throughput))
    
    log_line(LOG_FILE, "----- END LOG -----")

#######################################
##### Extract Transform Operators #####
#######################################

start_task = PythonOperator(
    task_id = 'start_task',
    python_callable=set_start,
    dag=dag

)

extract_transform_1 = PythonOperator(
    task_id='extract_transform_1',
    python_callable=mimic,
    op_kwargs={
        'name': 'extract_transform_1',
        'kernel': get_extract_transform_params
    },
    dag=dag,
)

extract_transform_2 = PythonOperator(
    task_id='extract_transform_2',
    python_callable=mimic,
    op_kwargs={
        'name': 'extract_transform_2',
        'kernel': get_extract_transform_params
    },
    dag=dag,
)

extract_transform_3 = PythonOperator(
    task_id='extract_transform_3',
    python_callable=mimic,
    op_kwargs={
        'name': 'extract_transform_3',
        'kernel': get_extract_transform_params
    },
    dag=dag,
)

###############################
##### Processor Operators #####
###############################

processor_1 = PythonOperator(
    task_id='processor_1',
    python_callable=mimic,
    op_kwargs={
        'name': 'processor_1',
        'kernel': get_processor_params
    },
    dag=dag,
)

processor_2 = PythonOperator(
    task_id='processor_2',
    python_callable=mimic,
    op_kwargs={
        'name': 'processor_2',
        'kernel': get_processor_params
    },
    dag=dag,
)

processor_3 = PythonOperator(
    task_id='processor_3',
    python_callable=mimic,
    op_kwargs={
        'name': 'processor_3',
        'kernel': get_processor_params
    },
    dag=dag,
)

processor_4 = PythonOperator(
    task_id='processor_4',
    python_callable=mimic,
    op_kwargs={
        'name': 'processor_4',
        'kernel': get_processor_params
    },
    dag=dag,
)

processor_5 = PythonOperator(
    task_id='processor_5',
    python_callable=mimic,
    op_kwargs={
        'name': 'processor_5',
        'kernel': get_processor_params
    },
    dag=dag,
)

processor_6 = PythonOperator(
    task_id='processor_6',
    python_callable=mimic,
    op_kwargs={
        'name': 'processor_6',
        'kernel': get_processor_params
    },
    dag=dag,
)

################################
##### Classifier Operators #####
################################

classifier_1 = PythonOperator(
    task_id='classifier_1',
    python_callable=mimic,
    op_kwargs={
        'name': 'classifier_1',
        'kernel': get_classifier_params
    },
    dag=dag,
)

classifier_2 = PythonOperator(
    task_id='classifier_2',
    python_callable=mimic,
    op_kwargs={
        'name': 'classifier_2',
        'kernel': get_classifier_params
    },
    dag=dag,
)

classifier_3 = PythonOperator(
    task_id='classifier_3',
    python_callable=mimic,
    op_kwargs={
        'name': 'classifier_3',
        'kernel': get_classifier_params
    },
    dag=dag,
)

classifier_4 = PythonOperator(
    task_id='classifier_4',
    python_callable=mimic,
    op_kwargs={
        'name': 'classifier_4',
        'kernel': get_classifier_params
    },
    dag=dag,
)

end_task = PythonOperator(
    task_id='end_task',
    python_callable=print_total_throughput,
    dag=dag,
)

start_task >> [extract_transform_1, extract_transform_2, extract_transform_3]
extract_transform_1 >> [processor_1, processor_2, processor_3] >> classifier_1
extract_transform_2 >> [processor_2, processor_3, processor_4, processor_5] 
[processor_2, processor_3, processor_4] >> classifier_2
[processor_1, processor_2, processor_3, processor_4] >> classifier_3
extract_transform_3 >> [processor_5, processor_6] >> classifier_4
[classifier_1, classifier_2, classifier_3, classifier_4] >> end_task

