import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator

with DAG(
    dag_id="load_data_regions_from_covid_api_dag",
    start_date=pendulum.today(),
    description="Script loads data about regions at https://covid-api.com/api/",
    schedule=None,
    tags=["practical tasks"]
) as dag:
    start_op = EmptyOperator(
        task_id="start"
    )

    load_regions_data_op = BashOperator(
        task_id="load_regions_data",
        bash_command="opt/airflow/scripts/fetch_covid_api.sh",
        env={"message": "Fetching data from the covid API..."}
    )

    finish_op = EmptyOperator(
        task_id="finish"
    )

    start_op >> load_regions_data_op >> finish_op
    