"""Generate CSV files from Data Warehouse for state-specific analysis

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


DEFAULT_ARGS = {
'owner': 'airflow',
'depends_on_past': False,
'retries':0,
'email_on_failure': False,
'email_on_retry': False
}

dag = DAG(
    'generate_analysis_csv_dag',
    default_args=DEFAULT_ARGS,
    start_date=datetime(2019,8,25),
    catchup=True,
    dagrun_timeout=timedelta(hours=2),
    #schedule_interval='0 3 * * *'
    #schedule_interval=timedelta(seconds=10)
    description='US visitors generate analysis csv files',
    schedule_interval=None
)


JOB_FLOW_OVERRIDES = {
    'Name' : 'usvisitors_analysiscsvs_etl',
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


ALL_CSV = [
{
    'Name': 'etl_usvisitors_fact',
    'ActionOnFailure': 'CONTINUE',
    'HadoopJarStep': {
        'Jar': 'command-runner.jar',
        'Args': [
            'spark-submit',
             '--deploy-mode',
             'client',
             '--master',
             'yarn',
             '/home/hadoop/python_apps/usvisitors_allstates_allmodes.py',
             '-p',
             s3data
        ]
    }
}
]


CA_CSV = [
{
    'Name': 'etl_usvisitors_fact',
    'ActionOnFailure': 'CONTINUE',
    'HadoopJarStep': {
        'Jar': 'command-runner.jar',
        'Args': [
            'spark-submit',
             '--deploy-mode',
             'client',
             '--master',
             'yarn',
             '/home/hadoop/python_apps/dice_usvisitors.py',
             '-y',
             '[2016]',
             '-m',
             '[6,7,8]',
             '-s',
             '[CA]',
             '-p',
             s3data
        ]
    }
}
]

NY_CSV = [
{
    'Name': 'etl_usvisitors_fact',
    'ActionOnFailure': 'CONTINUE',
    'HadoopJarStep': {
        'Jar': 'command-runner.jar',
        'Args': [
            'spark-submit',
             '--deploy-mode',
             'client',
             '--master',
             'yarn',
             '/home/hadoop/python_apps/dice_usvisitors.py',
             '-y',
             '[2016]',
             '-m',
             '[6,7,8]',
             '-s',
             '[NY]',
             '-p',
             s3data
        ]
    }
}
]



# Use wait_for_downstream to ensure sequential DAG runs
start_operator = DummyOperator(
    task_id='Begin_execution',
    dag=dag,
    wait_for_downstream=True
)

cluster_creator = EmrCreateJobFlowOperator(
    task_id='create_job_flow',
    job_flow_overrides=JOB_FLOW_OVERRIDES,
    aws_conn_id='aws_default',
    emr_conn_id='emr_default',
    dag=dag
)

add_all_task = MyEmrAddStepsOperator(
    task_id='add_allstep',
    job_flow_id="{{ task_instance.xcom_pull('create_job_flow', key='return_value') }}",
    aws_conn_id='aws_default',
    steps=ALL_CSV,
    retries=3,
    retry_delay=timedelta(minutes=5),
    dag=dag
)

watch_allstep_task = EmrStepSensor(
    task_id='watch_allstep',
    job_flow_id="{{ task_instance.xcom_pull('create_job_flow', key='return_value') }}",
    step_id="{{ task_instance.xcom_pull('add_allstep', key='return_value')[0] }}",
    aws_conn_id='aws_default',
    dag=dag
)

add_ca_task = MyEmrAddStepsOperator(
    task_id='add_castep',
    job_flow_id="{{ task_instance.xcom_pull('create_job_flow', key='return_value') }}",
    aws_conn_id='aws_default',
    steps=CA_CSV,
    retries=3,
    retry_delay=timedelta(minutes=5),
    dag=dag
)

watch_castep_task = EmrStepSensor(
    task_id='watch_castep',
    job_flow_id="{{ task_instance.xcom_pull('create_job_flow', key='return_value') }}",
    step_id="{{ task_instance.xcom_pull('add_castep', key='return_value')[0] }}",
    aws_conn_id='aws_default',
    dag=dag
)

add_ny_task = MyEmrAddStepsOperator(
    task_id='add_nystep',
    job_flow_id="{{ task_instance.xcom_pull('create_job_flow', key='return_value') }}",
    aws_conn_id='aws_default',
    steps=NY_CSV,
    retries=3,
    retry_delay=timedelta(minutes=5),
    dag=dag
)

watch_nystep_task = EmrStepSensor(
    task_id='watch_nystep',
    job_flow_id="{{ task_instance.xcom_pull('create_job_flow', key='return_value') }}",
    step_id="{{ task_instance.xcom_pull('add_nystep', key='return_value')[0] }}",
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

start_operator >> cluster_creator \
                >> add_all_task \
                >> watch_allstep_task \
                >> add_ca_task \
                >> watch_castep_task \
                >> add_ny_task\
                >> watch_nystep_task \
                >> cluster_remover \
                >> pause_task \
                >> end_operator

# Include a dependency on the completion of the pause_task to
# ensure that the catchup runs do not overlap and cause
# issues with the EC2 quotas
start_operator >> pause_task
