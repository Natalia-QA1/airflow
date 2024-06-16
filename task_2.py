import pendulum
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator


def run_task():
    print("task is running")


with DAG(
    dag_id="weekly_scheduled_dag",
    description="DAG which would be triggered every 2 hours \
        on weekdays from 11:00 to 19:00",
    start_date=pendulum.datetime(
        2024,
        6,
        16
    ),
    schedule_interval="0 */2 11-19 * 1-5",
    tags=["practical task"],
    catchup=False

) as dag:

    start_op = EmptyOperator(
        task_id="start"
    )

    run_task_op = PythonOperator(
        task_id="task",
        python_callable=run_task
    )

    finish_op = EmptyOperator(
        task_id="finish"
    )

    start_op >> run_task_op >> finish_op
    