import io
import logging

import pandas as pd
import pendulum
import requests
from airflow import DAG
from airflow.models import (
    Param,
    Variable,
)
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.trigger_rule import TriggerRule


def _check_version_existence(params):
    biogrid_version = params['version']

    postgres_hook = PostgresHook(postgres_conn_id='aws_postgres')
    connection = postgres_hook.get_conn()

    with connection.cursor() as cursor:
        cursor.execute('SELECT DISTINCT(bd."version") FROM biogrid_data bd;')
        loaded_versions = [version[0] for version in cursor.fetchall()]

        if biogrid_version not in loaded_versions:
            return 'load_data'
        else:
            return 'finish'


def load_biogrid(params, ti):
    biogrid_version = params['version']
    biogrid_url = Variable.get('biogrid_url')
    bucket = Variable.get('bucket_biogrid')
    s3_key = f'biogrid_v{biogrid_version.replace(".", "_")}.tab3.zip'
    ti.xcom_push(
        key='s3_key',
        value=s3_key
    )

    logging.info('Loading biogrid file...')
    response = requests.get(
        biogrid_url.format(version=biogrid_version),
        params={'downloadformat': 'zip'}
    )

    if response.status_code == 200:
        s3_hook = S3Hook(aws_conn_id='aws_s3')
        s3_client = s3_hook.get_conn()
        s3_client.put_object(
            Bucket=bucket,
            Key=s3_key,
            Body=response.content
        )
    else:
        logging.error('The specified version is not found')
        raise Exception()

    logging.info('Biogrid file has loaded')
    logging.info('Starting biogrid processing')


def ingest_data(params, ti):
    biogrid_version = params['version']
    s3_key = ti.xcom_pull(
        task_ids='load_data',
        key='s3_key'
    )

    bucket = Variable.get('bucket_biogrid')

    s3_hook = S3Hook(aws_conn_id='aws_s3')
    s3_client = s3_hook.get_conn()

    response = s3_client.get_object(
        Bucket=bucket,
        Key=s3_key
    )

    file_content = response['Body'].read()
    df = pd.read_csv(
        io.BytesIO(file_content),
        delimiter='\t',
        compression='zip',
        nrows=100
    )

    df = df.rename(
        lambda column_name: column_name.lower().replace(' ', '_').replace('#', '_').strip('_'),
        axis='columns'
    )

    df = df[[
        'biogrid_interaction_id',
        'biogrid_id_interactor_a',
        'biogrid_id_interactor_b',
    ]]

    df['version'] = biogrid_version

    logging.info('Biogrid file has been transformed')
    logging.info('Starting ingestion into database...')

    postgres_hook = PostgresHook(postgres_conn_id='aws_postgres')
    connection = postgres_hook.get_conn()

    insert_query = """
        INSERT INTO biogrid_data (
        biogrid_interaction_id, 
        biogrid_id_interactor_a, 
        biogrid_id_interactor_b, 
        version
        )
        VALUES (%s, %s, %s, %s)
        """

    with connection.cursor() as cursor:
        for index, row in df.iterrows():
            cursor.execute(insert_query, 
                           (
                                row['biogrid_interaction_id'],
                                row['biogrid_id_interactor_a'],
                                row['biogrid_id_interactor_b'],
                                row['version']
                           )
                           )
        connection.commit()

    logging.info('Data successfully ingested')


with DAG(
    dag_id='biogrid_loading_dag',
    start_date=pendulum.today(),
    schedule=None,
    tags=[
        'biogrid',
        'practical_task'
    ],
    description='A DAG to load biogrid from website into Postgres database',
    catchup=False,
    params={
        'version': Param('4.4.200', type='string'),
        'bucket_biogrid': Param('biogrid_data', type='string')
    }
) as dag:
    start_op = EmptyOperator(
        task_id='start'
    )

    check_version_existence_op = BranchPythonOperator(
        task_id='check_version_existence',
        python_callable=_check_version_existence
    )

    load_data_op = PythonOperator(
        task_id='load_data',
        python_callable=load_biogrid
    )

    ingest_data_op = PythonOperator(
        task_id='ingest_data',
        python_callable=ingest_data
    )

    trigger_function_op = PostgresOperator(
        task_id='trigger_function',
        sql='SELECT get_biogrid_interactors();',
        postgres_conn_id='aws_postgres'
    )

    finish_op = EmptyOperator(
        task_id='finish',
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS
    )

    start_op >> check_version_existence_op >> load_data_op >> ingest_data_op >> trigger_function_op >> finish_op
    start_op >> check_version_existence_op >> finish_op
