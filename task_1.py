import pendulum
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator


def run_weekly_task():
    print("weekly task")


with DAG(
    dag_id="weekly_scheduled_dag",
    description="DAG which would be triggered every week starting \
     from 15th of June and ending on  13th of July",
    start_date=pendulum.datetime(
        2024,
        6,
        15
    ),
    end_date=pendulum.datetime(
        2024,
        7,
        13
    ),
    schedule_interval="@weekly",
    tags=["practical task"],
    catchup=False

) as dag:

    start_op = EmptyOperator(
        task_id="start"
    )

    run_task_op = PythonOperator(
        task_id="week_task",
        python_callable=run_weekly_task
    )

    finish_op = EmptyOperator(
        task_id="finish"
    )

    start_op >> run_task_op >> finish_op
