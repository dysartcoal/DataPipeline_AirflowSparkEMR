"""ETL for US visitors fixed dimension tables from csv on S3 to parquet on S3.

The DAG runs once and carries out the following steps:

- check for source data
- spin up the EMR cluster
- add steps to the EMR cluster to generate the fixed dimension tables
- watch the steps for completion
- terminate the cluster

There is a task to wait for the cluster termination to avoid issues with requesting
EC2 resources exceeding the existing quota.

"""
import airflow
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.operators.emr_create_job_flow_operator import EmrCreateJobFlowOperator
from airflow.contrib.operators.emr_add_steps_operator import EmrAddStepsOperator
from airflow.contrib.sensors.emr_step_sensor import EmrStepSensor
from airflow.contrib.operators.emr_terminate_job_flow_operator import EmrTerminateJobFlowOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.capstone_plugin import MyEmrAddStepsOperator
from airflow.operators.capstone_plugin import S3DataExistsOperator

from datetime import datetime, timedelta
import time

s3data = 's3://dysartcoal-dend-uswest2/capstone_etl/data'
s3bucket = 'dysartcoal-dend-uswest2'
lookup_prefix = 'capstone_etl/data/lookup_data'


DEFAULT_ARGS = {
'owner': 'airflow',
'depends_on_past': False,
'retries':0,
'email_on_failure': False,
'email_on_retry': False
}

dag = DAG(
    'usvisitors_dim_dag',
    default_args=DEFAULT_ARGS,
    start_date=datetime(2019,8,25), # Start on 7th Feb to process Jan data
    catchup=False,
    dagrun_timeout=timedelta(hours=2),
    #schedule_interval='0 3 * * *'
    #schedule_interval=timedelta(seconds=10)
    description='US visitors dimensions ETL',
    schedule_interval=None  # Trigger manually
)


JOB_FLOW_OVERRIDES = {
    'Name' : 'usvisitors_dim_etl',
    'LogUri' : 's3://dysartcoal-dend-uswest2/emr-log',
    'ReleaseLabel' : 'emr-5.25.0',
    'Instances' : {
      'InstanceGroups': [
            {
                'Name': 'Master nodes',
                'Market': 'ON_DEMAND',
                'InstanceRole': 'MASTER',
                'InstanceType': 'm5.xlarge',
                'InstanceCount': 1,
            },
            {
                'Name': 'Slave nodes',
                'Market': 'ON_DEMAND',
                'InstanceRole': 'CORE',
                'InstanceType': 'm5.xlarge',
                'InstanceCount': 1,
            }
        ],
        'Ec2KeyName': 'spark-cluster',
        'KeepJobFlowAliveWhenNoSteps': True,
        'TerminationProtected': False
    },
    'BootstrapActions': [
        {
            'Name': 'copy python jars to local;',
            'ScriptBootstrapAction': {
                'Path': 's3://dysartcoal-dend-uswest2/capstone_etl/awsemr/bootstrap_action.sh'
            }
        },
    ],
    'Applications':[{
        'Name': 'Spark'
    },{
        'Name': 'Livy'
    },{
        'Name': 'Hadoop'
    },{
        'Name': 'Zeppelin'
    },{
        'Name': 'Ganglia'
    }],
    'JobFlowRole':'EMR_EC2_DefaultRole',
    'ServiceRole':'EMR_DefaultRole'
}


ETL_DIM = [
{
    'Name': 'etl_usvisitors_dim',
    'ActionOnFailure': 'CONTINUE',
    'HadoopJarStep': {
        'Jar': 'command-runner.jar',
        'Args': [
            'spark-submit',
             '--deploy-mode',
             'client',
             '--master',
             'yarn',
             '/home/hadoop/python_apps/usvisitors_dimensions.py',
             '-p',
             s3data
        ]
    }
}
]


start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

check_lookupi94port_s3  = S3DataExistsOperator(
    task_id='check_lookupi94port_on_s3',
    dag=dag,
    aws_conn_id="aws_default",
    bucket=s3bucket,
    prefix=lookup_prefix,
    key = 'i94port_data.csv'
)

cluster_creator = EmrCreateJobFlowOperator(
    task_id='create_job_flow',
    job_flow_overrides=JOB_FLOW_OVERRIDES,
    aws_conn_id='aws_default',
    emr_conn_id='emr_default',
    dag=dag
)

add_dimstep_task = MyEmrAddStepsOperator(
    task_id='add_dimstep',
    job_flow_id="{{ task_instance.xcom_pull('create_job_flow', key='return_value') }}",
    aws_conn_id='aws_default',
    steps=ETL_DIM,
    retries=3,
    retry_delay=timedelta(minutes=5),
    dag=dag
)

watch_dimstep_task = EmrStepSensor(
    task_id='watch_dimstep',
    job_flow_id="{{ task_instance.xcom_pull('create_job_flow', key='return_value') }}",
    step_id="{{ task_instance.xcom_pull('add_dimstep', key='return_value')[0] }}",
    aws_conn_id='aws_default',
    dag=dag
)

cluster_remover = EmrTerminateJobFlowOperator(
    task_id='remove_cluster',
    job_flow_id="{{ task_instance.xcom_pull('create_job_flow', key='return_value') }}",
    aws_conn_id='aws_default',
    trigger_rule='all_done',  # Run shut down regardless of success
    retries=3,
    retry_delay=timedelta(minutes=5),
    dag=dag
)

def pause(minutes):
    '''Sleep for the given number of minutes'''
    time.sleep(minutes*60)

pause_task = PythonOperator(
    # Catch up of dates can cause EC2 quotas to be exceeded so pause for termination
    task_id='pause_for_termination',
    python_callable=pause,
    op_kwargs={'minutes': 15},
    dag=dag
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

start_operator >> check_lookupi94port_s3 \
                >> cluster_creator \
                >> add_dimstep_task \
                >> watch_dimstep_task \
                >> cluster_remover \
                >> pause_task \
                >> end_operator
