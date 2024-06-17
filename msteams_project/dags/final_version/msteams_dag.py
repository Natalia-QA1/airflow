import os
import sys

sys.path.append(os.path.abspath(os.path.dirname(__file__)))

from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import (
    Param
)
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import (
    PythonOperator,
    BranchPythonOperator
)
from airflow.utils.trigger_rule import TriggerRule

from msteams_project_dag_functions import (
    download_quote,
    download_image,
    load_into_aws_s3,
    load_into_aws_rds,
    check_date,
    send_msg
)

with DAG(
        dag_id='send_massage_msteams_dag_final_version',
        start_date=datetime(
            year=2024,
            month=6,
            day=1
        ),
        schedule_interval="0 12 * * *",  # send every day at 12 a.m.
        tags=[
            'send_message_msteams'
        ],
        description='A DAG to send message to MS Teams channel using webhook. \
        Message consists of quote and picture. ',
        catchup=False,
        dagrun_timeout=timedelta(
            minutes=5
        ),
        params={
            'bucket_name': Param('images-msteams', type="string"),
            'message_color': Param('#F8C471', type="string")
        },

) as dag:
    start_op = EmptyOperator(
        task_id='start'
    )

    check_date_op = BranchPythonOperator(
        task_id='check_date',
        python_callable=check_date,
    )

    download_quote_op = PythonOperator(
        task_id='download_quote',
        python_callable=download_quote,
    )

    download_image_op = PythonOperator(
        task_id='download_image',
        python_callable=download_image,
    )

    load_image_s3_op = PythonOperator(
        task_id='load_img_s3',
        python_callable=load_into_aws_s3,
    )

    load_quote_rds_op = PythonOperator(
        task_id='load_quote_rds',
        python_callable=load_into_aws_rds,
    )

    send_message_op = PythonOperator(
        task_id='send_message',
        python_callable=send_msg,
    )

    finish_op = EmptyOperator(
        task_id="finish",
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS
    )

    start_op >> check_date_op
    check_date_op >> finish_op
    check_date_op >> [
        download_quote_op,
        download_image_op
    ] >> load_image_s3_op >> load_quote_rds_op >> send_message_op >> finish_op

