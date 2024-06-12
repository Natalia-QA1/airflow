import pendulum
from airflow.models import DAG
from airflow.operators.email import EmailOperator
from airflow.operators.empty import EmptyOperator

with DAG (
    dag_id="email_sender",
    start_date=pendulum.today(),
    description="test DAG for email message sender",
    catchup=False,

) as dag:
    start_op = EmptyOperator(
        task_id="start"
    )

    send_email_op = EmailOperator(
        task_id='send_email',
        to='ananiev4nat@yandex.ru',
        subject='Airflow Alert',
        html_content='<p>Job has finished.</p>',
        conn_id='smtp_default'
    )

    finish_op = EmptyOperator(
        task_id="finish"
    )
