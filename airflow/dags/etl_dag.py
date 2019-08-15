import airflow
from airflow import DAG
from airflow.contrib.operators.emr_create_job_flow_operator import EmrCreateJobFlowOperator
from airflow.contrib.operators.emr_add_steps_operator import EmrAddStepsOperator
from airflow.contrib.sensors.emr_step_sensor import EmrStepSensor
from airflow.contrib.operators.emr_terminate_job_flow_operator import EmrTerminateJobFlowOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.capstone_plugin import MyEmrAddStepsOperator
from airflow.operators.capstone_plugin import S3DataExistsOperator

from datetime import datetime, timedelta

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
    'emr_test_manual8',
    default_args=DEFAULT_ARGS,
    start_date=datetime(2016,3,7), # Start on 7th Feb to process Jan data
    end_date=datetime(2016,3,7), # Currently only have data until Dec 2016
    catchup=True,
    dagrun_timeout=timedelta(hours=2),
    #schedule_interval='0 3 * * *'
    #schedule_interval=timedelta(seconds=10)
    description='ETL i94 pipeline',
    schedule_interval=schedule_interval
)


TEST_I94PORT = [
{
    'Name': 'test_i94port',
    'ActionOnFailure': 'CONTINUE',
    'HadoopJarStep': {
        'Jar': 'command-runner.jar',
        'Args': [
            'spark-submit',
             '--deploy-mode',
             'client',
             '--master',
             'yarn',
             '/home/hadoop/python_apps/read_i94port.py',
             '-y',
             '2016',
             '-m',
             '1',
             '-p',
             s3data
        ]
    }
}
]


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

watch_i94_step_task = EmrStepSensor(
    task_id='watch_i94_step',
    job_flow_id="{{ task_instance.xcom_pull('create_job_flow', key='return_value') }}",
    step_id="{{ task_instance.xcom_pull('add_i94step', key='return_value')[0] }}",
    aws_conn_id='aws_default',
    retries=3,
    retry_delay=timedelta(minutes=5),
    dag=dag
)

cluster_remover = EmrTerminateJobFlowOperator(
    task_id='remove_cluster',
    job_flow_id="{{ task_instance.xcom_pull('create_job_flow', key='return_value') }}",
    aws_conn_id='aws_default',
    trigger_rule='all_failed',
    retries=3,
    retry_delay=timedelta(minutes=5),
    dag=dag
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

start_operator >> [check_lookupi94port_s3,
                    check_lookupi94citres_s3,
                    check_analyticsflightno_s3,
                    check_sascsv_s3] \
                >> cluster_creator \
                >> add_i94step_task \
                >> watch_i94_step_task \
                >> cluster_remover \
                >> end_operator
