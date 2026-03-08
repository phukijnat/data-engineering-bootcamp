from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.utils import timezone

def _hello():
    return "Hello"

with DAG(
    dag_id="my_first_dag",
    schedule=None,
    start_date=timezone.datetime(2026, 3, 6),
):

    t1 = EmptyOperator(task_id="t1")
    t2 = EmptyOperator(task_id="t2")
    t3 = PythonOperator(
        task_id = "t3",
        python_callable=_hello
    ) 

    t1 >> t2
    t1 >> t3