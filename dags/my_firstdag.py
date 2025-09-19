from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

default_args = {
    "owner": "you",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(seconds=3),
}

def make_bread(**kwargs):
    print("Making bread...")
    #raise Exception("Oven broke!")
    return "sourdough"
 

with DAG(
    dag_id="bakery_dag",
    default_args=default_args,
    description="Simple bakery example: open → make → pack",
    schedule_interval="@daily",
    start_date=datetime(2025, 8, 29),
    catchup=False,
    tags=["example", "bakery"],
) as dag:

    start = EmptyOperator(task_id="bakery_open")

    make_bread_task = PythonOperator(
        task_id="make_bread",
        python_callable=make_bread,
    )

    pack_bread = BashOperator(
        task_id="pack_bread",
        bash_command='echo "Packing {{ ti.xcom_pull(task_ids=\'make_bread\') }}" && sleep 1'
    )
    deliver_bread=BashOperator(
        task_id="delivery_bread",
        bash_command='echo "bread_delivered"'
    )
    
    sell_bread = BashOperator(
        task_id="sell_breads",
        bash_command="echo 'breads are selled '")
        
    clean_kitchen=BashOperator(
         task_id="Kitchen_cleaning_task",
         bash_command="echo 'Kitchen has been cleaned successfully'"
    )

    start >> make_bread_task >> pack_bread >> [deliver_bread,sell_bread] >> clean_kitchen
