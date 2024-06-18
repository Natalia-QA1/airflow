import pendulum
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

with DAG(
    dag_id='load_test_data_contacts_dag_v1',
    start_date=pendulum.today(),
    description='A DAG to load test data into contacts table',
    schedule_interval=None,
    tags=["practical tasks"]
) as dag:
    start_op = EmptyOperator(
            task_id="start"
        )

    load_test_data_op = PostgresOperator(
        task_id='load_test_data_task_v1',
        postgres_conn_id='postgres',
        sql="SELECT load_test_data_contacts();",
        dag=dag,
    )

    finish_op = EmptyOperator(
        task_id="finish"
    )

    start_op >> load_test_data_op >> finish_op
