import random

import pendulum
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.utils.trigger_rule import TriggerRule


def _choose_sales():
    available_sales = [
        'fetch_sales_old', 
        'fetch_sales_new'
    ]
    
    return random.choice(available_sales)


with DAG(
    dag_id="practical_task_4_dag",
    description="DAG with complex dependencies.",
    start_date=pendulum.datetime(
        2024,
        6,
        15
    ),
    schedule_interval="@weekly",
    tags=["practical task"],
    catchup=False

) as dag:

    start_op = EmptyOperator(
        task_id="start"
    )

    pick_op = BranchPythonOperator(
        task_id="pick_erp_system",
        python_callable=_choose_sales
    )

    fetch_w_op = EmptyOperator(
        task_id="fetch_weather"
    )

    sales_n_op = EmptyOperator(
        task_id="fetch_sales_new"
    )
    sales_o_op = EmptyOperator(
        task_id="fetch_sales_old"
    )

    clean_sales_n_op = EmptyOperator(
        task_id="clean_sales_new"
    )
    clean_sales_o_op = EmptyOperator(
        task_id="clean_sales_old"
    )

    clean_w_op = EmptyOperator(
        task_id="clean_weather"
    )

    join_op = EmptyOperator(
        task_id="join_datasets",
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS
    )

    train_op = EmptyOperator(
        task_id="train_model"
    )
    deploy_op = EmptyOperator(
        task_id="deploy_model"
    )

    start_op >> [pick_op, fetch_w_op]
    pick_op >> sales_n_op >> clean_sales_n_op >> join_op
    pick_op >> sales_o_op >> clean_sales_o_op >> join_op
    fetch_w_op >> clean_w_op >> join_op
    join_op >> train_op >> deploy_op
