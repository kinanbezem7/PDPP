from airflow.models import DAG
from datetime import datetime
from datetime import timedelta
from airflow.operators.bash_operator import BashOperator

default_args = {
    'owner': 'Admin',
    'depends_on_past': False,
    'email': ['admin@example.org'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'start_date': datetime(2020, 1, 1),
    'retry_delay': timedelta(minutes=5),
    'end_date': datetime(2023, 1, 1),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'wait_for_downstream': False,
    # 'dag': dag,
    # 'trigger_rule': 'all_success'
}
with DAG(dag_id='test_dags',
         default_args=default_args,
         schedule_interval='0 0 * * *',
         catchup=False,
         tags=['test']
         ) as dag:
    # Define the tasks. Here we are going to define only one bash operator
    test_task = BashOperator(
        task_id='spark',
        bash_command='python /home/kinan/airflow/dags/spark.py',
        dag=dag)
    
