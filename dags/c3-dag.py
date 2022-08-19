from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.operators.emr_create_job_flow_operator import EmrCreateJobFlowOperator
from airflow.contrib.operators.emr_add_steps_operator import EmrAddStepsOperator
from airflow.contrib.sensors.emr_step_sensor import EmrStepSensor
from airflow.contrib.operators.emr_terminate_job_flow_operator import EmrTerminateJobFlowOperator


SPARK_STEPS = [
    {
        "Name": "Move raw data from S3 to emr",
        "ActionOnFailure": "CANCEL_AND_WAIT",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": [
                "sudo",
                "aws",
                "s3",
                "cp",
                "--recursive",
                "s3://sb-de-c3/scripts/",
                "/home/hadoop/"
            ],
        },
    },
        {
        "Name": "Install Requirements",
        "ActionOnFailure": "CANCEL_AND_WAIT",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": [
                "sudo",
                "python3",
                "-m",
                "pip",
                "install",
                "-r",
                "/home/hadoop/requirements.txt"
            ],
        },
    },
    {
        "Name": "ELTL",
        "ActionOnFailure": "CANCEL_AND_WAIT",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": [
                "sudo",
                "spark-submit",
                "/home/hadoop/main.py"
            ],
        },
    }
]

JOB_FLOW_OVERRIDES = {
    "Name": "Reddit Place",
    "ReleaseLabel": "emr-5.36.0",
    "Applications": [{"Name": "Spark"}, {"Name": "Zeppelin"}],
    "Instances": {
        "InstanceGroups": [
            {
                "Name": "Master Instance Group",
                "Market": "SPOT",
                "InstanceRole": "MASTER",
                "InstanceType": "c5.4xlarge",
                "InstanceCount": 1,
            },
            {
                "Name": "Core Instance Group",
                "Market": "SPOT",
                "InstanceRole": "CORE",
                "InstanceType": "c5.4xlarge",
                "InstanceCount": 1,
            },
        ],
        "KeepJobFlowAliveWhenNoSteps": True,
        "TerminationProtected": False,
    },
    "JobFlowRole": "EMR_EC2_DefaultRole",
    "ServiceRole": "EMR_DefaultRole",
}

default_arguments = {
    'owner': 'Dakota Brown',
    'email': ['dakotacbrown@gmail.com'],
    'start_date': datetime(2022, 8, 3),
    "depends_on_past": True,
    "wait_for_downstream": True,
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

etl_dag = DAG(
    'reddit_dag',
    default_args=default_arguments,
    description='A DAG for reddit place data',
    schedule_interval='@once'
)

t0 = DummyOperator(
    task_id="start_data_pipeline",
    dag=etl_dag
)

# Create an EMR cluster
t1 = EmrCreateJobFlowOperator(
    task_id="create_emr_cluster",
    job_flow_overrides=JOB_FLOW_OVERRIDES,
    aws_conn_id="aws_default",
    emr_conn_id="emr_default",
    dag=etl_dag,
)

# Add your steps to the EMR cluster
t2 = EmrAddStepsOperator(
    task_id="add_steps",
    job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
    aws_conn_id="aws_default",
    steps=SPARK_STEPS,
    dag=etl_dag,
)

last_step = len(SPARK_STEPS) - 1
# wait for the steps to complete
t3 = EmrStepSensor(
    task_id="watch_step",
    job_flow_id="{{ task_instance.xcom_pull('create_emr_cluster', key='return_value') }}",
    step_id="{{ task_instance.xcom_pull(task_ids='add_steps', key='return_value')["
    + str(last_step)
    + "] }}",
    aws_conn_id="aws_default",
    dag=etl_dag,
)

# Terminate the EMR cluster
t4 = EmrTerminateJobFlowOperator(
    task_id="terminate_emr_cluster",
    job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
    aws_conn_id="aws_default",
    dag=etl_dag,
)

t5 = DummyOperator(
    task_id="end_data_pipeline",
    dag=etl_dag
)

t0 >> t1 >> t2 >> t3 >> t4 >> t5
