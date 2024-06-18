import pendulum
from airflow import DAG
from airflow.models.baseoperator import chain_linear
from airflow.operators.empty import EmptyOperator


with DAG(
    dag_id="practical_task_3_dag",
    description="DAG with complex dependencies (chain_linear).",
    start_date=pendulum.datetime(
        2024,
        6,
        15
    ),
    schedule_interval="@weekly",
    tags=["practical task"],
    catchup=False

) as dag:

    task_1_1_op = EmptyOperator(
        task_id="task_1_1"
    )

    task_1_2_op = EmptyOperator(
        task_id="task_1_2"
    )
    task_1_3_op = EmptyOperator(
        task_id="task_1_3"
    )

    task_2_1_op = EmptyOperator(
        task_id="task_2_1"
    )
    task_2_2_op = EmptyOperator(
        task_id="task_2_2"
    )

    task_2_3_op = EmptyOperator(
        task_id="task_2_3"
    )

    task_3_1_op = EmptyOperator(
        task_id="task_3_1",
    )

    task_3_2_op = EmptyOperator(
        task_id="task_3_2"
    )

    chain_linear(
        [task_1_1_op, task_1_2_op, task_1_3_op],
        [task_2_1_op, task_2_2_op, task_2_3_op],
        [task_3_1_op, task_3_2_op]
    )
    