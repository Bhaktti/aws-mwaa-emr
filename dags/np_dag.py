import mwaalib.workflow_lib as etlclient
import logging
import requests
import json
import pandas as pd
import boto3
from botocore.exceptions import NoCredentialsError, PartialCredentialsError


from airflow import DAG
from airflow.utils import dates
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from mwaalib.emr_submit_and_monitor_step import EmrSubmitAndMonitorStepOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': dates.days_ago(0),
    'retries': 0,
    'retry_delay': timedelta(minutes=2),
    'provide_context': True
}

# Initialize the DAG
# Concurrency --> Number of tasks allowed to run concurrently
dag = DAG('national_park_dag', concurrency=3, schedule_interval=None, default_args=default_args)
region = etlclient.detect_running_region()
etlclient.client(region_name=region)


# Creates an EMR cluster
def create_emr(**kwargs):
    cluster_id = etlclient.create_cluster(region_name=region, cluster_name='mwaa_emr_cluster', num_core_nodes=2)
    return cluster_id


# Waits for the EMR cluster to be ready to accept jobs
def wait_for_completion(**kwargs):
    ti = kwargs['ti']
    cluster_id = ti.xcom_pull(task_ids='create_cluster')
    logging.info(f"Waiting for EMR cluster {cluster_id} to be ready...")
    try:
        etlclient.wait_for_cluster_creation(cluster_id)
        logging.info(f"EMR cluster {cluster_id} is ready.")
    except Exception as e:
        logging.error(f"Error while waiting for EMR cluster {cluster_id} to be ready: {e}")
        raise


# Terminates the EMR cluster
def terminate_emr(**kwargs):
    ti = kwargs['ti']
    cluster_id = ti.xcom_pull(task_ids='create_cluster')
    etlclient.terminate_cluster(cluster_id)

# Monitors Step function execution
def monitor_step_function(**kwargs):
    ti = kwargs['ti']
    execution_arn = ti.xcom_pull(task_ids='trigger_stepfunctions')
    response = etlclient.monitor_stepfunction_run(execution_arn)
    return response

# Starts the Step Function
def trigger_step_function(**kwargs):
    # get the Step function arn
    statemachine_arn = kwargs['stateMachineArn']
    sfn_execution_arn = etlclient.start_execution(statemachine_arn)
    return sfn_execution_arn

def extract_load_park_activity_data():
    response = requests.get(
        'https://developer.nps.gov/api/v1/activities/parks',
        headers={'accept': 'application/json'},
        params={'api_key': '7fRCZjCFt0SLvkSwkYQwhPcqKGsdunZXaho0R7fX'}
    )

    data = response.json()
    df = pd.json_normalize(data['data'], 'parks', ['id', 'name'], record_prefix='park_')
    df.to_csv('/tmp/parks_data.csv', index=False)

    s3 = boto3.client('s3')
    bucket_name = etlclient.get_demo_bucket_name()
    s3.upload_file('/tmp/parks_data.csv', bucket_name, 'raw/parks_data.csv')

# Define the individual tasks using Python Operators
create_cluster = PythonOperator(
    task_id='create_cluster',
    python_callable=create_emr,
    dag=dag)

wait_for_cluster_completion = PythonOperator(
    task_id='wait_for_cluster_completion',
    python_callable=wait_for_completion,
    dag=dag)

# Define the PythonOperator
extract_load_activity_data_task = PythonOperator(
    task_id='extract_load_data',
    python_callable=extract_load_park_activity_data,
    dag=dag,
)

preprocess_parks_activities = EmrSubmitAndMonitorStepOperator(
    task_id="preprocess_parks_activities",
    steps=[
        {
            "Name": "preprocess_parks_activities",
            "ActionOnFailure": "CONTINUE",
            "HadoopJarStep": {
                "Jar": "command-runner.jar",
                "Args": [
                    "spark-submit",
                    "--conf",
                    "deploy-mode=cluster",
                    "s3://{}/scripts/preprocess_parks_activities_data.py".format(etlclient.get_demo_bucket_name()),
                    etlclient.get_demo_bucket_name()
                ],
            },
        }
    ],
    job_flow_id="{{ task_instance.xcom_pull(task_ids='create_cluster', key='return_value') }}",
    aws_conn_id="aws_default",
    check_interval=int("30"),
    dag=dag
)

preprocess_parks_revenue = EmrSubmitAndMonitorStepOperator(
    task_id="preprocess_parks_revenue",
    steps=[
        {
            "Name": "preprocess_parks_revenue",
            "ActionOnFailure": "CONTINUE",
            "HadoopJarStep": {
                "Jar": "command-runner.jar",
                "Args": [
                    "spark-submit",
                    "--conf",
                    "deploy-mode=cluster",
                    "s3://{}/scripts/preprocess_parks_revenue.py".format(etlclient.get_demo_bucket_name()),
                    etlclient.get_demo_bucket_name()
                ],
            },
        }
    ],
    job_flow_id="{{ task_instance.xcom_pull(task_ids='create_cluster', key='return_value') }}",
    aws_conn_id="aws_default",
    check_interval=int("30"),
    dag=dag
)

preprocess_parks_visits_by_month = EmrSubmitAndMonitorStepOperator(
    task_id="preprocess_parks_visits_by_month",
    steps=[
        {
            "Name": "preprocess_parks_visits_by_month",
            "ActionOnFailure": "CONTINUE",
            "HadoopJarStep": {
                "Jar": "command-runner.jar",
                "Args": [
                    "spark-submit",
                    "--conf",
                    "deploy-mode=cluster",
                    "s3://{}/scripts/preprocess_parks_visits_by_month.py".format(etlclient.get_demo_bucket_name()),
                    etlclient.get_demo_bucket_name()
                ],
            },
        }
    ],
    job_flow_id="{{ task_instance.xcom_pull(task_ids='create_cluster', key='return_value') }}",
    aws_conn_id="aws_default",
    check_interval=int("30"),
    dag=dag
)

preprocess_parks_visits_by_year = EmrSubmitAndMonitorStepOperator(
    task_id="preprocess_parks_visits_by_year",
    steps=[
        {
            "Name": "preprocess_parks_visits_by_year",
            "ActionOnFailure": "CONTINUE",
            "HadoopJarStep": {
                "Jar": "command-runner.jar",
                "Args": [
                    "spark-submit",
                    "--conf",
                    "deploy-mode=cluster",
                    "s3://{}/scripts/preprocess_parks_visits_by_year.py".format(etlclient.get_demo_bucket_name()),
                    etlclient.get_demo_bucket_name()
                ],
            },
        }
    ],
    job_flow_id="{{ task_instance.xcom_pull(task_ids='create_cluster', key='return_value') }}",
    aws_conn_id="aws_default",
    check_interval=int("30"),
    #job_name="preprocess_ratings",
    dag=dag
)

terminate_cluster = PythonOperator(
    task_id='terminate_cluster',
    python_callable=terminate_emr,
    trigger_rule='all_done',
    dag=dag)

trigger_sfn = PythonOperator(
    task_id='trigger_stepfunctions',
    python_callable=trigger_step_function,
    op_kwargs={'stateMachineArn': etlclient.get_stepfunction_arn()},
    dag=dag)

monitor_sfn = PythonOperator(
    task_id='monitor_stepfunctions',
    python_callable=monitor_step_function,
    dag=dag)

# construct the DAG by setting the dependencies
create_cluster >> wait_for_cluster_completion
wait_for_cluster_completion >> extract_load_activity_data_task
extract_load_activity_data_task >> preprocess_parks_activities
extract_load_activity_data_task >> preprocess_parks_revenue
extract_load_activity_data_task >> preprocess_parks_visits_by_month
extract_load_activity_data_task >> preprocess_parks_visits_by_year

# Ensure all preprocessing tasks are completed before terminating the cluster
[preprocess_parks_activities, preprocess_parks_revenue, preprocess_parks_visits_by_month, preprocess_parks_visits_by_year] >> terminate_cluster

terminate_cluster >> trigger_sfn >> monitor_sfn

