from __future__ import print_function
from builtins import range
from airflow.operators.python_operator import PythonOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator,
                                S3DataExistsOperator)
from airflow.operators.dummy_operator import DummyOperator
from airflow.models import DAG
from datetime import datetime, timedelta


import os
import time
from pprint import pprint

schedule_interval = '0 0 1 * *'  # Schedule to run at midnight on the 1st of each month
s3_sas_data = 'capstone_etl/data/sas_data/{prev_execution_date.year}/{prev_execution_date.month:02d}'

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 0,
    'catchup': False
}


dag = DAG(
    dag_id='test_s3exists_dag31',
    start_date=datetime(2016,2,1), # Start on first of Feb to process Jan data
    end_date=datetime(2016,2,1), # Currently only have data until Dec 2016
    catchup=True,
    default_args=default_args,
    description='Test out the s3 checks',
    schedule_interval='@monthly')


start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

check_lookupi94port_s3  = S3DataExistsOperator(
    task_id='check_lookupi94port_on_s3',
    dag=dag,
    aws_conn_id="aws_conn_id",
    bucket='dysartcoal-dend-uswest2',
    prefix='capstone_etl/data/lookup_data',
    key = 'i94port_codes.csv'
)

check_lookupi94citres_s3  = S3DataExistsOperator(
    task_id='check_lookupi94citres_on_s3',
    dag=dag,
    aws_conn_id="aws_conn_id",
    bucket='dysartcoal-dend-uswest2',
    prefix='capstone_etl/data/lookup_data',
    key = 'i94cit_i94res_codes.csv'
)

check_analyticsflightno_s3  = S3DataExistsOperator(
    task_id='check_analyticsflightno_on_s3',
    dag=dag,
    aws_conn_id="aws_conn_id",
    bucket='dysartcoal-dend-uswest2',
    prefix='capstone_etl/data/analytics_data/flight',
    key = 'flightno.csv'
)

check_sascsv_s3  = S3DataExistsOperator(
    task_id='check_sascsv_on_s3',
    dag=dag,
    aws_conn_id="aws_conn_id",
    bucket='dysartcoal-dend-uswest2',
    prefix=s3_sas_data,
    wildcard_key = '*.csv'
)


def print_context(ds, **kwargs):
    pprint(kwargs)
    print('The year is {}'.format(ds[:4]))
    print('The month is {}'.format(ds[5:7]))
    print('The time stamp for this execution is: {}'.format(ds))
    for key, value in os.environ.items():
        print(f'{key}:{value}')

    return 'Whatever you return gets printed in the logs'

run_this = PythonOperator(
    task_id='print_the_context',
    provide_context=True,
    python_callable=print_context,
    dag=dag)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

start_operator >> check_lookupi94port_s3 >> end_operator
start_operator >> check_lookupi94citres_s3 >> end_operator
start_operator >> check_analyticsflightno_s3 >> end_operator
start_operator >> check_sascsv_s3  >> end_operator
start_operator >> run_this >> end_operator
