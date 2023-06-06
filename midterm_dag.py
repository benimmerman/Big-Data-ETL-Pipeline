import airflow
from airflow import DAG
from datetime import timedelta
import json
    
from airflow.contrib.operators.emr_add_steps_operator import EmrAddStepsOperator

from airflow.contrib.sensors.emr_step_sensor import EmrStepSensor

from airflow.operators.python_operator import PythonOperator

# from airflow.contrib.operators.emr_create_job_flow_operator import EmrCreateJobFlowOperator

DEFAULT_ARGS = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': airflow.utils.dates.days_ago(0),
    'email': ['<your email>'],
    'email_on_failure': False,
    'email_on_retry': False,
}

CLUSTER_ID = '<your EMR cluster ID>'

def retrieve_s3_files(**kwargs):
    data_files = kwargs['dag_run'].conf
    kwargs['ti'].xcom_push(key = 'data_files', value = data_files)

SPARK_STEPS = [
    {
        'Name': 'wcd_midterm_job',
        'ActionOnFailure': 'CONTINUE',
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': [
                '/usr/bin/spark-submit',
                '--master', 'yarn',
                '--deploy-mode','client',
                '--num-executors', '2',
                '--driver-memory', '512m',
                '--executor-memory','3g',
                '--executor-cores','2',
                '<path of s3 location for midterm_workflow.py>',
                '-p', json.dumps({'input_file_paths': "{{ task_instance.xcom_pull('parse_request', key='data_files') }}", # jinja template
                'output_path': '<s3 output path>' })
            ]
        }
    }
]


dag = DAG(
    'midterm_dag',
    default_args = DEFAULT_ARGS,
    dagrun_timeout = timedelta(hours=2),
    schedule_interval = None,
)

parse_request = PythonOperator(
    task_id = 'parse_request',
    provide_context = True,
    python_callable = retrieve_s3_files,
    dag=dag
)

step_adder = EmrAddStepsOperator(
    task_id = 'add_steps',
    job_flow_id = CLUSTER_ID,
    steps = SPARK_STEPS,
    dag = dag
)

step_checker = EmrStepSensor(
    task_id = 'watch_step',
    job_flow_id = CLUSTER_ID,
    step_id = "{{ task_instance.xcom_pull('add_steps', key='return_value')[0]}}",
    aws_conn_id = 'aws_default',
    dag = dag
)

parse_request >> step_adder >> step_checker
