import pendulum
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.common.sql.sensors.sql import SqlSensor


def _success_criteria(record):
    return bool(record)


def _failure_criteria(record):
    return not bool(record)


with DAG(
        dag_id='test_data_load_sensor_dag',
        start_date=pendulum.today(),
        schedule_interval=None,
        tags=["practical tasks"]
) as dag:
    start_op = EmptyOperator(
        task_id="start"
    )

    sensor_op = SqlSensor(
        task_id="check_test_data_loading",
        conn_id="postgres",
        sql="SELECT 1 FROM contacts LIMIT 1",
        success=_success_criteria,
        failure=_failure_criteria,
        fail_on_empty=False,
        poke_interval=20,
        mode="reschedule",
        timeout=60 * 5,
    )

    finish_op = EmptyOperator(
        task_id="finish"
    )

    start_op >> sensor_op >> finish_op
