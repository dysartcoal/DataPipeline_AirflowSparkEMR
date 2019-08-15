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

schedule_interval = '0 0 7 * *'  # Schedule to run at midnight on the 7th of each month
s3data = 's3://dysartcoal-dend-uswest2/capstone_etl/data'
s3bucket = 'dysartcoal-dend-uswest2'
flight_prefix = 'capstone_etl/data/analytics_data/flight'
lookup_prefix = 'capstone_etl/data/lookup_data'
sas_prefix = 'capstone_etl/data/sas_data/{prev_execution_date.year}/{prev_execution_date.month:02d}'


DEFAULT_ARGS = {
'owner': 'airflow',
'depends_on_past': False,
'email': ['jkldatascience@gmail.com'],
'retries':0,
'email_on_failure': False,
'email_on_retry': False
}

dag = DAG(
    'etl_i94_dag1',
    default_args=DEFAULT_ARGS,
    start_date=datetime(2016,5,7), # Start on 7th Feb to process Jan data
    end_date=datetime(2016,5,7), # Currently only have data until Dec 2016
    catchup=True,
    dagrun_timeout=timedelta(hours=2),
    #schedule_interval='0 3 * * *'
    #schedule_interval=timedelta(seconds=10)
    description='ETL i94 pipeline',
    schedule_interval=schedule_interval
)


JOB_FLOW_OVERRIDES = {
    'Name' : 'i94_etl',
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
                'InstanceCount': 3,
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


ETL_I94 = [
{
    'Name': 'etl_i94',
    'ActionOnFailure': 'CONTINUE',
    'HadoopJarStep': {
        'Jar': 'command-runner.jar',
        'Args': [
            'spark-submit',
             '--deploy-mode',
             'client',
             '--master',
             'yarn',
             '/home/hadoop/python_apps/etl_i94.py',
             '-y',
             '{{prev_execution_date.year}}',
             '-m',
             '{{prev_execution_date.month}}',
             '-p',
             s3data
        ]
    }
}
]

ETL_ROWCHECK = [
{
    'Name': 'etl_rowcheck',
    'ActionOnFailure': 'CONTINUE',
    'HadoopJarStep': {
        'Jar': 'command-runner.jar',
        'Args': [
            'spark-submit',
             '--deploy-mode',
             'client',
             '--master',
             'yarn',
             '/home/hadoop/python_apps/checki94rows_sas_parquet.py',
             '-y',
             '{{prev_execution_date.year}}',
             '-m',
             '{{prev_execution_date.month}}',
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
    key = 'i94port_codes.csv'
)

check_lookupi94citres_s3  = S3DataExistsOperator(
    task_id='check_lookupi94citres_on_s3',
    dag=dag,
    aws_conn_id="aws_default",
    bucket=s3bucket,
    prefix=lookup_prefix,
    key = 'i94cit_i94res_codes.csv'
)

check_analyticsflightno_s3  = S3DataExistsOperator(
    task_id='check_analyticsflightno_on_s3',
    dag=dag,
    aws_conn_id="aws_default",
    bucket=s3bucket,
    prefix=flight_prefix,
    key = 'flightno.csv'
)

check_sascsv_s3  = S3DataExistsOperator(
    task_id='check_sascsv_on_s3',
    dag=dag,
    aws_conn_id="aws_default",
    bucket=s3bucket,
    prefix=sas_prefix,
    wildcard_key = '*.csv'
)

cluster_creator = EmrCreateJobFlowOperator(
    task_id='create_job_flow',
    job_flow_overrides=JOB_FLOW_OVERRIDES,
    aws_conn_id='aws_default',
    emr_conn_id='emr_default',
    dag=dag
)

add_i94step_task = MyEmrAddStepsOperator(
    task_id='add_i94step',
    job_flow_id="{{ task_instance.xcom_pull('create_job_flow', key='return_value') }}",
    aws_conn_id='aws_default',
    steps=ETL_I94,
    retries=3,
    retry_delay=timedelta(minutes=5),
    dag=dag
)

watch_i94step_task = EmrStepSensor(
    task_id='watch_i94step',
    job_flow_id="{{ task_instance.xcom_pull('create_job_flow', key='return_value') }}",
    step_id="{{ task_instance.xcom_pull('add_i94step', key='return_value')[0] }}",
    aws_conn_id='aws_default',
    dag=dag
)

add_checkstep_task = MyEmrAddStepsOperator(
    task_id='add_checkstep',
    job_flow_id="{{ task_instance.xcom_pull('create_job_flow', key='return_value') }}",
    aws_conn_id='aws_default',
    steps=ETL_ROWCHECK,
    retries=3,
    retry_delay=timedelta(minutes=5),
    dag=dag
)

watch_checkstep_task = EmrStepSensor(
    task_id='watch_checkstep',
    job_flow_id="{{ task_instance.xcom_pull('create_job_flow', key='return_value') }}",
    step_id="{{ task_instance.xcom_pull('add_i94step', key='return_value')[0] }}",
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

start_operator >> [check_lookupi94port_s3,
                    check_lookupi94citres_s3,
                    check_analyticsflightno_s3,
                    check_sascsv_s3] \
                >> cluster_creator \
                >> add_i94step_task \
                >> watch_i94step_task \
                >> add_checkstep_task \
                >> watch_checkstep_task \
                >> cluster_remover \
                >> pause_task \
                >> end_operator
