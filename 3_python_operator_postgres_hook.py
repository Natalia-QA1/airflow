import pendulum
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from sqlalchemy import create_engine
from sqlalchemy.exc import (
    OperationalError,
    IntegrityError,
    DataError,
    ProgrammingError,
    InvalidRequestError
)
from sqlalchemy.orm import sessionmaker


def load_test_data():
    postgres_hook = PostgresHook(
        postgres_conn_id="postgres"
    )
    conn = postgres_hook.get_connection(postgres_hook.postgres_conn_id)
    user = conn.login
    password = conn.password
    host = conn.host
    port = 5432
    dbname = conn.schema

    engine = create_engine(
        f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{dbname}"
    )

    Session = sessionmaker(bind=engine)
    session = Session()

    try:
        session.execute("SELECT load_test_data_contacts();")
        session.commit()
    except (
            OperationalError,
            IntegrityError,
            DataError,
            ProgrammingError,
            InvalidRequestError
    ) as e:
        session.rollback()
        print(f"Database error occurred: {e}")
    finally:
        session.close()


with DAG(
    dag_id="load_test_data_contacts_dag_v2",
    start_date=pendulum.today(),
    description="A DAG to load test data into contacts table",
    schedule_interval=None,
    tags=["practical tasks"]
) as dag:
    start_op = EmptyOperator(
            task_id="start"
        )

    load_test_data_op = PythonOperator(
        task_id="load_test_data_task_v2",
        python_callable=load_test_data
    )

    finish_op = EmptyOperator(
        task_id="finish"
    )

    start_op >> load_test_data_op >> finish_op
    